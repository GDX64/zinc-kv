//! How a single commit decides between success and `Conflict`, including when another
//! writer slips in between the precondition check and the `Create`.

mod common;

use std::sync::Arc;

use common::{log_files, open, store, text, FaultStore, BACKENDS};
use object_store::ObjectStore;
use zinc_kv::CasError;

#[tokio::test]
async fn stale_cas_reports_current_version() {
    for backend in BACKENDS {
        let store = store(backend, "stale_cas");
        let db = open(&store).await;

        let v1 = db.put("k", text("a")).await.unwrap();
        let v2 = db.cas("k", Some(v1), Some(text("b"))).await.unwrap();

        let err = db.cas("k", Some(v1), Some(text("c"))).await.unwrap_err();
        assert!(
            matches!(err, CasError::Conflict { current: Some(v) } if v == v2),
            "{backend:?}: {err:?}"
        );
        let err = db.cas("k", None, Some(text("c"))).await.unwrap_err();
        assert!(
            matches!(err, CasError::Conflict { current: Some(v) } if v == v2),
            "{backend:?}: {err:?}"
        );
        let entry = db.get("k").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("b"), v2), "{backend:?}");

        db.delete("k").await.unwrap();
        let err = db.cas("k", Some(v2), Some(text("c"))).await.unwrap_err();
        assert!(
            matches!(err, CasError::Conflict { current: None }),
            "{backend:?}: {err:?}"
        );
        // Conflicts write nothing: put, cas, delete.
        assert_eq!(log_files(&store).await.len(), 3, "{backend:?}");
    }
}

#[tokio::test]
async fn delete_then_create_gets_higher_version() {
    for backend in BACKENDS {
        let store = store(backend, "recreate");
        let db = open(&store).await;

        let v1 = db.cas("lock", None, Some(text("node-a"))).await.unwrap();
        db.delete("lock").await.unwrap();
        assert_eq!(db.get("lock").await.unwrap(), None, "{backend:?}");

        let v3 = db.cas("lock", None, Some(text("node-b"))).await.unwrap();
        assert!(v3 > v1, "{backend:?}");
        let entry = db.get("lock").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("node-b"), v3), "{backend:?}");
    }
}

#[tokio::test]
async fn cas_delete_checks_version() {
    for backend in BACKENDS {
        let store = store(backend, "cas_delete");
        let db = open(&store).await;

        let v1 = db.put("k", text("a")).await.unwrap();
        let v2 = db.put("k", text("b")).await.unwrap();

        let err = db.cas("k", Some(v1), None).await.unwrap_err();
        assert!(
            matches!(err, CasError::Conflict { current: Some(v) } if v == v2),
            "{backend:?}: {err:?}"
        );
        assert!(db.get("k").await.unwrap().is_some(), "{backend:?}");

        db.cas("k", Some(v2), None).await.unwrap();
        assert_eq!(db.get("k").await.unwrap(), None, "{backend:?}");
        // The tombstone counts as absent for a later create.
        db.cas("k", None, Some(text("c"))).await.unwrap();
    }
}

/// A has checked its precondition and is about to `Create`; B changes the same key first.
/// A must lose with B's version, not overwrite B.
#[tokio::test]
async fn retry_window_same_key_conflicts() {
    for backend in BACKENDS {
        let inner = store(backend, "retry_same_key");
        let faulty = FaultStore::new(inner.clone());
        let a = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        let b = open(&inner).await;

        let v1 = b.put("k", text("0")).await.unwrap();

        let paused = faulty.pause_next_create();
        let task = tokio::spawn(async move { a.cas("k", Some(v1), Some(text("a"))).await });
        paused.reached.await.unwrap();
        let vb = b.put("k", text("b")).await.unwrap();
        paused.resume.send(()).unwrap();

        let err = task.await.unwrap().unwrap_err();
        assert!(
            matches!(err, CasError::Conflict { current: Some(v) } if v == vb),
            "{backend:?}: {err:?}"
        );
        assert_eq!(b.get("k").await.unwrap().unwrap().value, text("b"));
        assert_eq!(log_files(&inner).await.len(), 2, "{backend:?}");
    }
}

/// Same race, but B changes another key: A rechecks only B's commit and takes the next number.
#[tokio::test]
async fn retry_window_other_key_succeeds() {
    for backend in BACKENDS {
        let inner = store(backend, "retry_other_key");
        let faulty = FaultStore::new(inner.clone());
        let a = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        let b = open(&inner).await;

        let v1 = b.put("k", text("0")).await.unwrap();

        let paused = faulty.pause_next_create();
        let task = tokio::spawn(async move { a.cas("k", Some(v1), Some(text("a"))).await });
        paused.reached.await.unwrap();
        let vb = b.put("other", text("b")).await.unwrap();
        paused.resume.send(()).unwrap();

        let va = task.await.unwrap().unwrap();
        assert!(va > vb, "{backend:?}");
        let entry = b.get("k").await.unwrap().unwrap();
        assert_eq!((entry.value, entry.version), (text("a"), va), "{backend:?}");
        assert_eq!(b.get("other").await.unwrap().unwrap().version, vb);
        assert_eq!(log_files(&inner).await.len(), 3, "{backend:?}");
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
