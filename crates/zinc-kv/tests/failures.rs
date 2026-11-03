//! Damaged logs and misbehaving stores: every case must end in a correct answer or a
//! typed error, never in a wrong value or a silently lost write.

mod common;

use std::{path::PathBuf, sync::Arc, time::Duration};

use common::{
    commit_name, disk, log_files, open, store, text, CreateFault, FaultStore, BACKENDS, PREFIX,
};
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use zinc_kv::{CasError, Error};

/// A store on disk holding commits 1..=3 (put a, put b, put c), plus its log directory.
async fn three_commits_on_disk(name: &str) -> (Arc<dyn ObjectStore>, PathBuf) {
    let (store, root) = disk(name);
    let store: Arc<dyn ObjectStore> = Arc::new(store);
    let db = open(&store).await;
    for key in ["a", "b", "c"] {
        db.put(key, text(key)).await.unwrap();
    }
    (store, root.join(PREFIX).join("log"))
}

fn assert_missing(result: Result<impl std::fmt::Debug, Error>, number: u64) {
    match result {
        Err(Error::MissingCommit { number: n }) if n == number => {}
        other => panic!("expected MissingCommit {{ number: {number} }}, got {other:?}"),
    }
}

fn assert_cas_missing(result: Result<impl std::fmt::Debug, CasError>, number: u64) {
    match result {
        Err(CasError::Other(Error::MissingCommit { number: n })) if n == number => {}
        other => panic!("expected MissingCommit {{ number: {number} }}, got {other:?}"),
    }
}

/// A hole in the middle: even a key that lives in a newer commit is not read around it.
#[tokio::test]
async fn hole_in_log_is_an_error() {
    let (store, log) = three_commits_on_disk("hole_middle").await;
    std::fs::remove_file(log.join(commit_name(2))).unwrap();
    let db = open(&store).await;

    assert_missing(db.get("a").await, 2);
    assert_missing(db.get("c").await, 2);
    assert_missing(db.get("absent").await, 2);
    assert_missing(db.put("d", text("d")).await, 2);
    assert_missing(db.delete("a").await, 2);
    assert_cas_missing(db.cas("absent", None, Some(text("x"))).await, 2);
    // Nothing was written past the hole.
    assert_eq!(log_files(&store).await, [commit_name(1), commit_name(3)]);
}

#[tokio::test]
async fn missing_first_commit_is_an_error() {
    let (store, log) = three_commits_on_disk("hole_first").await;
    std::fs::remove_file(log.join(commit_name(1))).unwrap();
    let db = open(&store).await;

    assert_missing(db.get("c").await, 1);
    assert_missing(db.put("d", text("d")).await, 1);
    assert_cas_missing(db.cas("a", None, Some(text("x"))).await, 1);
}

/// A commit that is listed but gone by the time it is read.
#[tokio::test]
async fn commit_vanishing_after_listing_is_an_error() {
    for backend in BACKENDS {
        let inner = store(backend, "vanish");
        let seed = open(&inner).await;
        for key in ["a", "b", "c"] {
            seed.put(key, text(key)).await.unwrap();
        }
        let faulty = FaultStore::new(inner.clone());
        faulty.vanish_on_get(&commit_name(2));
        let db = open(&(faulty as Arc<dyn ObjectStore>)).await;

        assert_missing(db.get("a").await, 2);
        assert_cas_missing(db.cas("a", None, Some(text("x"))).await, 2);
        // Commit 3 alone answers for "c".
        assert_eq!(db.get("c").await.unwrap().unwrap().value, text("c"));
    }
}

#[tokio::test]
async fn corrupt_commit_is_an_error() {
    let (store, log) = three_commits_on_disk("corrupt").await;
    std::fs::write(log.join(commit_name(2)), "not json").unwrap();
    let db = open(&store).await;

    assert!(
        matches!(db.get("a").await, Err(Error::Corrupt { number: 2, .. })),
        "{:?}",
        db.get("a").await
    );
    assert!(matches!(
        db.cas("a", None, Some(text("x"))).await,
        Err(CasError::Other(Error::Corrupt { number: 2, .. }))
    ));
    // Keys found before reaching the bad commit are still readable.
    assert_eq!(db.get("c").await.unwrap().unwrap().value, text("c"));
}

#[tokio::test]
async fn unknown_field_is_corrupt() {
    let (store, log) = three_commits_on_disk("unknown_field").await;
    std::fs::write(
        log.join(commit_name(2)),
        r#"{"v":1,"id":"x","ops":[],"expires":5}"#,
    )
    .unwrap();
    let db = open(&store).await;

    assert!(matches!(
        db.get("a").await,
        Err(Error::Corrupt { number: 2, .. })
    ));
}

#[tokio::test]
async fn unknown_format_version_is_an_error() {
    let (store, log) = three_commits_on_disk("unknown_format").await;
    std::fs::write(log.join(commit_name(2)), r#"{"v":2,"anything":[1,2]}"#).unwrap();
    let db = open(&store).await;

    assert!(matches!(
        db.get("a").await,
        Err(Error::UnsupportedFormat {
            number: 2,
            version: 2
        })
    ));
}

#[tokio::test]
async fn stray_objects_in_log_are_ignored() {
    for backend in BACKENDS {
        let store = store(backend, "stray");
        let db = open(&store).await;
        let v1 = db.put("a", text("a")).await.unwrap();

        for name in ["notes.txt", "3.json", "00000000000000000002.json.tmp", "0000000000000000000x.json"] {
            store
                .put(&Path::from(format!("{PREFIX}/log/{name}")), text("junk").into())
                .await
                .unwrap();
        }

        let entry = db.get("a").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("a"), v1), "{backend:?}");
        let v2 = db.put("b", text("b")).await.unwrap();
        assert!(v2 > v1);
        assert_eq!(db.get("b").await.unwrap().unwrap().version, v2);
        assert!(log_files(&store).await.contains(&commit_name(2)), "{backend:?}");
    }
}

/// The write landed but the store reported an error: the writer recognises its own commit.
#[tokio::test]
async fn landed_write_reported_as_error_succeeds_once() {
    for backend in BACKENDS {
        let inner = store(backend, "landed_error");
        let faulty = FaultStore::new(inner.clone());
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;

        faulty.fail_next_create(CreateFault::FailAfterWrite);
        let v1 = db.put("k", text("a")).await.unwrap();

        faulty.fail_next_create(CreateFault::FailAfterWrite);
        let v2 = db.cas("k", Some(v1), Some(text("b"))).await.unwrap();

        let entry = db.get("k").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("b"), v2), "{backend:?}");
        assert_eq!(log_files(&inner).await.len(), 2, "{backend:?}");
    }
}

/// The write landed but the store reported `AlreadyExists` (as after a client-side retry).
/// This must be a success, not a `Conflict` against our own commit.
#[tokio::test]
async fn landed_write_reported_as_already_exists_succeeds_once() {
    for backend in BACKENDS {
        let inner = store(backend, "landed_exists");
        let faulty = FaultStore::new(inner.clone());
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;

        faulty.fail_next_create(CreateFault::AlreadyExistsAfterWrite);
        let v1 = db.cas("lock", None, Some(text("me"))).await.unwrap();

        faulty.fail_next_create(CreateFault::AlreadyExistsAfterWrite);
        let v2 = db.put("other", text("x")).await.unwrap();

        assert!(v2 > v1);
        assert_eq!(db.get("lock").await.unwrap().unwrap().version, v1);
        assert_eq!(log_files(&inner).await.len(), 2, "{backend:?}");
    }
}

/// A single failed `Create` is retried and the write lands exactly once.
#[tokio::test]
async fn transient_write_failure_is_retried() {
    for backend in BACKENDS {
        let inner = store(backend, "transient_write");
        let faulty = FaultStore::new(inner.clone());
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;

        faulty.fail_next_create(CreateFault::FailBeforeWrite);
        let v1 = db.put("k", text("a")).await.unwrap();
        faulty.fail_next_create(CreateFault::FailBeforeWrite);
        let v2 = db.cas("k", Some(v1), Some(text("b"))).await.unwrap();

        let entry = db.get("k").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("b"), v2), "{backend:?}");
        assert_eq!(log_files(&inner).await.len(), 2, "{backend:?}");
    }
}

/// A store that keeps failing: the store error is returned and nothing is written.
#[tokio::test]
async fn persistent_write_failure_returns_error() {
    for backend in BACKENDS {
        let inner = store(backend, "persistent_write");
        let faulty = FaultStore::new(inner.clone());
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;

        faulty.fail_every_create(Some(CreateFault::FailBeforeWrite));
        assert!(matches!(
            db.put("k", text("a")).await,
            Err(Error::Store(_))
        ));
        assert!(matches!(
            db.cas("k", None, Some(text("a"))).await,
            Err(CasError::Other(Error::Store(_)))
        ));
        assert!(log_files(&inner).await.is_empty(), "{backend:?}");
        assert_eq!(db.get("k").await.unwrap(), None);

        faulty.fail_every_create(None);
        db.put("k", text("b")).await.unwrap();
        assert_eq!(db.get("k").await.unwrap().unwrap().value, text("b"));
    }
}

/// The `Create` reported an error, and the request landed afterwards. The writer must
/// claim that commit: not write a duplicate, and not report a `Conflict` against itself.
#[tokio::test]
async fn write_landing_after_error_is_claimed() {
    for backend in BACKENDS {
        let inner = store(backend, "land_later");
        let faulty = FaultStore::new(inner.clone());
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;

        faulty.fail_next_create(CreateFault::FailThenLandLater);
        let v1 = db.cas("lock", None, Some(text("me"))).await.unwrap();

        faulty.fail_next_create(CreateFault::FailThenLandLater);
        let v2 = db.put("k", text("a")).await.unwrap();

        assert!(v2 > v1);
        assert_eq!(db.get("lock").await.unwrap().unwrap().version, v1);
        assert_eq!(db.get("k").await.unwrap().unwrap().version, v2);
        assert_eq!(log_files(&inner).await.len(), 2, "{backend:?}");
    }
}

#[tokio::test]
async fn failed_listing_returns_error() {
    for backend in BACKENDS {
        let inner = store(backend, "failed_list");
        open(&inner).await.put("k", text("a")).await.unwrap();
        let faulty = FaultStore::new(inner.clone());
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;

        faulty.fail_list(true);
        assert!(matches!(db.get("k").await, Err(Error::Store(_))));
        assert!(matches!(db.put("k", text("b")).await, Err(Error::Store(_))));
        assert!(matches!(
            db.cas("k", None, Some(text("b"))).await,
            Err(CasError::Other(Error::Store(_)))
        ));
        assert_eq!(log_files(&inner).await.len(), 1, "{backend:?}");

        faulty.fail_list(false);
        assert_eq!(db.get("k").await.unwrap().unwrap().value, text("a"));
    }
}

/// A competitor wins every number: the writer gives up with `Contention` in bounded time.
#[tokio::test]
async fn endless_contention_gives_up() {
    for backend in BACKENDS {
        let inner = store(backend, "contention");
        let faulty = FaultStore::new(inner.clone());
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        faulty.steal_every_create();

        let result = tokio::time::timeout(Duration::from_secs(30), db.put("k", text("a")))
            .await
            .expect("retries must be bounded");
        assert!(matches!(result, Err(Error::Contention)), "{result:?}");
        assert_eq!(db.get("k").await.unwrap(), None, "{backend:?}");
    }
}
