//! How a transaction commit decides between success and `Conflict`, including when another
//! writer slips in between the snapshot and the `Create`.

mod common;

use std::sync::Arc;

use common::{log_files, open, store, text, update, FaultStore, BACKENDS};
use object_store::ObjectStore;
use zinc_kv::Error;

fn assert_conflict<T: std::fmt::Debug>(result: zinc_kv::Result<T>, key: &str, ctx: &str) {
    match result {
        Err(Error::Conflict { key: k }) if k == key => {}
        other => panic!("{ctx}: expected Conflict on {key:?}, got {other:?}"),
    }
}

/// A key read by the transaction is changed by another writer before the commit: put over
/// a value, create over an absent read, delete over a value. Each conflict writes nothing.
#[tokio::test]
async fn changed_read_key_conflicts() {
    for backend in BACKENDS {
        let store = store(backend, "changed_read");
        let db = open(&store).await;
        let ctx = format!("{backend:?}");
        db.put("k", text("a")).await.unwrap();

        let mut tx = db.begin();
        assert_eq!(tx.get("k").await.unwrap().unwrap().value, text("a"));
        db.put("k", text("b")).await.unwrap();
        tx.put("k", text("c"));
        assert_conflict(tx.commit().await, "k", &ctx);

        let mut tx = db.begin();
        assert_eq!(tx.get("absent").await.unwrap(), None);
        db.put("absent", text("x")).await.unwrap();
        tx.put("absent", text("y"));
        assert_conflict(tx.commit().await, "absent", &ctx);

        let mut tx = db.begin();
        tx.get("k").await.unwrap().unwrap();
        db.delete("k").await.unwrap();
        tx.put("other", text("z"));
        assert_conflict(tx.commit().await, "k", &ctx);

        assert_eq!(db.get("k").await.unwrap(), None, "{ctx}");
        assert_eq!(db.get("absent").await.unwrap().unwrap().value, text("x"));
        assert_eq!(db.get("other").await.unwrap(), None, "{ctx}");
        // put a, put b, put absent, delete k.
        assert_eq!(log_files(&store).await.len(), 4, "{ctx}");
    }
}

/// Commits to keys the transaction did not read, and blind writes, never conflict.
#[tokio::test]
async fn unrelated_and_blind_writes_do_not_conflict() {
    for backend in BACKENDS {
        let store = store(backend, "unrelated");
        let db = open(&store).await;
        db.put("k", text("a")).await.unwrap();

        let mut tx = db.begin();
        tx.get("k").await.unwrap();
        let other = db.put("other", text("b")).await.unwrap();
        tx.put("k", text("c"));
        let epoch = tx.commit().await.unwrap().epoch.unwrap();
        assert!(epoch > other, "{backend:?}");

        // Writes without reads have no dependencies, even on the key they write.
        let mut tx = db.begin();
        tx.put("k", text("d"));
        db.put("k", text("e")).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(db.get("k").await.unwrap().unwrap().value, text("d"));
    }
}

#[tokio::test]
async fn read_only_transaction_writes_nothing() {
    for backend in BACKENDS {
        let store = store(backend, "read_only");
        let db = open(&store).await;
        db.put("k", text("a")).await.unwrap();

        let mut tx = db.begin();
        tx.get("k").await.unwrap();
        db.put("k", text("b")).await.unwrap();
        // Nothing to write, so nothing to conflict with.
        assert_eq!(tx.commit().await.unwrap().epoch, None, "{backend:?}");
        assert_eq!(log_files(&store).await.len(), 2, "{backend:?}");
    }
}

#[tokio::test]
async fn reads_share_one_snapshot() {
    for backend in BACKENDS {
        let store = store(backend, "snapshot");
        let db = open(&store).await;
        let va = db.put("a", text("1")).await.unwrap();
        db.put("b", text("1")).await.unwrap();

        let mut tx = db.begin();
        assert_eq!(tx.get("a").await.unwrap().unwrap().version, va);
        db.put("b", text("2")).await.unwrap();
        // Still the value at the snapshot, so a commit would conflict on `b`.
        assert_eq!(tx.get("b").await.unwrap().unwrap().value, text("1"));
        tx.put("a", text("2"));
        let ctx = format!("{backend:?}");
        assert_conflict(tx.commit().await, "b", &ctx);
    }
}

#[tokio::test]
async fn reading_own_write_is_an_error() {
    let store = store(common::Backend::Memory, "read_own_write");
    let db = open(&store).await;
    let mut tx = db.begin();
    tx.put("k", text("a"));
    let result = tx.get("k").await;
    assert!(
        matches!(&result, Err(Error::ReadAfterWrite { key }) if key == "k"),
        "{result:?}"
    );
}

#[tokio::test]
async fn delete_then_create_gets_higher_version() {
    for backend in BACKENDS {
        let store = store(backend, "recreate");
        let db = open(&store).await;

        let (read, v1) = update(&db, "lock", Some(text("node-a"))).await.unwrap();
        assert_eq!(read, None);
        let (read, _) = update(&db, "lock", None).await.unwrap();
        assert_eq!(read.unwrap().version, v1, "{backend:?}");
        assert_eq!(db.get("lock").await.unwrap(), None, "{backend:?}");

        // The tombstone reads as absent.
        let (read, v3) = update(&db, "lock", Some(text("node-b"))).await.unwrap();
        assert_eq!(read, None, "{backend:?}");
        assert!(v3 > v1, "{backend:?}");
        let entry = db.get("lock").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("node-b"), v3), "{backend:?}");
    }
}

/// A has read and is about to `Create`; B changes the same key first. A must lose, not
/// overwrite B.
#[tokio::test]
async fn retry_window_same_key_conflicts() {
    for backend in BACKENDS {
        let inner = store(backend, "retry_same_key");
        let faulty = FaultStore::new(inner.clone());
        let a = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        let b = open(&inner).await;

        b.put("k", text("0")).await.unwrap();

        let paused = faulty.pause_next_create();
        let task = tokio::spawn(async move { update(&a, "k", Some(text("a"))).await });
        paused.reached.await.unwrap();
        b.put("k", text("b")).await.unwrap();
        paused.resume.send(()).unwrap();

        assert_conflict(task.await.unwrap(), "k", &format!("{backend:?}"));
        assert_eq!(b.get("k").await.unwrap().unwrap().value, text("b"));
        assert_eq!(log_files(&inner).await.len(), 2, "{backend:?}");
    }
}

/// Same race, but B changes another key: A rechecks only B's commit and takes the next
/// number. This is not a transaction retry: A's read is still provably current.
#[tokio::test]
async fn retry_window_other_key_succeeds() {
    for backend in BACKENDS {
        let inner = store(backend, "retry_other_key");
        let faulty = FaultStore::new(inner.clone());
        let a = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        let b = open(&inner).await;

        b.put("k", text("0")).await.unwrap();

        let paused = faulty.pause_next_create();
        let task = tokio::spawn(async move { update(&a, "k", Some(text("a"))).await });
        paused.reached.await.unwrap();
        let vb = b.put("other", text("b")).await.unwrap();
        paused.resume.send(()).unwrap();

        let (_, va) = task.await.unwrap().unwrap();
        assert!(va > vb, "{backend:?}");
        let entry = b.get("k").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("a"), va), "{backend:?}");
        assert_eq!(b.get("other").await.unwrap().unwrap().version, vb);
        assert_eq!(log_files(&inner).await.len(), 3, "{backend:?}");
    }
}

/// The listing lags behind completed commits (hides everything after commit 1). Reads
/// must still see the latest value, a transaction on it must commit, and a change made
/// after the snapshot must still be found at commit time.
#[tokio::test]
async fn stale_listing_still_reads_and_writes_current_state() {
    for backend in BACKENDS {
        let inner = store(backend, "stale_listing");
        let writer = open(&inner).await;
        writer.put("lock", text("first")).await.unwrap();
        let second = writer.put("lock", text("second")).await.unwrap();

        let faulty = FaultStore::new(inner.clone());
        faulty.cap_listing_at(1);
        let reader = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;

        let entry = reader.get("lock").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("second"), second), "{backend:?}");

        let (read, third) = update(&reader, "lock", Some(text("third"))).await.unwrap();
        assert_eq!(read.unwrap().version, second, "{backend:?}");
        assert!(third > second);
        assert_eq!(writer.get("lock").await.unwrap().unwrap().version, third);

        let mut tx = reader.begin();
        tx.get("lock").await.unwrap();
        writer.put("lock", text("fourth")).await.unwrap();
        tx.put("lock", text("mine"));
        assert_conflict(tx.commit().await, "lock", &format!("{backend:?}"));
        assert_eq!(log_files(&inner).await.len(), 4, "{backend:?}");
    }
}

/// An unconditional put that loses the race for its number retries and lands after B.
#[tokio::test]
async fn put_retries_after_lost_race() {
    for backend in BACKENDS {
        let inner = store(backend, "put_retry");
        let faulty = FaultStore::new(inner.clone());
        let a = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        let b = open(&inner).await;

        let paused = faulty.pause_next_create();
        let task = tokio::spawn(async move { a.put("k", text("a")).await });
        paused.reached.await.unwrap();
        let vb = b.put("k", text("b")).await.unwrap();
        paused.resume.send(()).unwrap();

        let va = task.await.unwrap().unwrap();
        assert!(va > vb, "{backend:?}");
        let entry = b.get("k").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("a"), va), "{backend:?}");
    }
}
