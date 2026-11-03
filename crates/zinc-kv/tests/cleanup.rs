//! Cleanup deletes commits and checkpoints no reader needs. Operations that stall past the
//! retention must fail with `Expired`, never succeed invisibly or read around a gap.

mod common;

use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::Bytes;
use common::{commit_name, log_files, open, store, text, FaultStore, BACKENDS, PREFIX};
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use zinc_kv::{Db, Error, Version, CHECKPOINT_INTERVAL};

const KEYS: usize = 7;
const NOW: Duration = Duration::ZERO;
const HOUR: Duration = Duration::from_secs(3600);

type Model = HashMap<String, (Bytes, Version)>;

/// Writes `commits` commits over a few keys (every fifth a delete) into `model`.
async fn fill(db: &Db, commits: u64, model: &mut Model) {
    for i in 0..commits {
        let key = format!("k{}", i as usize % KEYS);
        if i % 5 == 4 {
            db.delete(&key).await.unwrap();
            model.remove(&key);
        } else {
            let value = text(&format!("{i}"));
            let v = db.put(&key, value.clone()).await.unwrap();
            model.insert(key, (value, v));
        }
    }
}

async fn assert_state(db: &Db, model: &Model, ctx: &str) {
    for k in 0..KEYS {
        let key = format!("k{k}");
        let got = db.get(&key).await.unwrap().map(|e| (e.value, e.version));
        assert_eq!(got, model.get(&key).cloned(), "{ctx}: {key}");
    }
}

async fn checkpoint_files(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let listing = store
        .list_with_delimiter(Some(&Path::from(format!("{PREFIX}/checkpoints"))))
        .await
        .unwrap();
    let mut names: Vec<String> = listing
        .objects
        .iter()
        .map(|o| o.location.filename().unwrap().to_owned())
        .collect();
    names.sort();
    names
}

fn names(numbers: impl IntoIterator<Item = u64>) -> Vec<String> {
    numbers.into_iter().map(commit_name).collect()
}

/// Everything up to the newest checkpoint goes, except that checkpoint; stray files stay;
/// reads, writes and later checkpoints carry on.
#[tokio::test]
async fn cleanup_deletes_up_to_the_newest_old_checkpoint() {
    for backend in BACKENDS {
        let store = store(backend, "cleanup_basic");
        let db = open(&store).await;
        let mut model = Model::new();
        fill(&db, 2 * CHECKPOINT_INTERVAL + 50, &mut model).await;
        let stray = Path::from(format!("{PREFIX}/log/notes.txt"));
        store.put(&stray, text("keep me").into()).await.unwrap();

        db.cleanup(NOW).await.unwrap();
        let ctx = format!("{backend:?}");
        let mut expected = names(2 * CHECKPOINT_INTERVAL + 1..=2 * CHECKPOINT_INTERVAL + 50);
        expected.push("notes.txt".into());
        assert_eq!(log_files(&store).await, expected, "{ctx}");
        assert_eq!(
            checkpoint_files(&store).await,
            names([2 * CHECKPOINT_INTERVAL]),
            "{ctx}"
        );

        assert_state(&db, &model, &ctx).await;
        assert_state(&open(&store).await, &model, &ctx).await;
        fill(&db, CHECKPOINT_INTERVAL, &mut model).await;
        assert_eq!(
            checkpoint_files(&store).await,
            names([2 * CHECKPOINT_INTERVAL, 3 * CHECKPOINT_INTERVAL]),
            "{ctx}"
        );
        assert_state(&open(&store).await, &model, &ctx).await;
    }
}

/// Right after the newest checkpoint was cleaned up to, the next commit's predecessor is
/// gone: the writer must still see its own commit as valid.
#[tokio::test]
async fn first_commit_after_cleanup_is_visible() {
    for backend in BACKENDS {
        let store = store(backend, "cleanup_first_commit");
        let db = open(&store).await;
        let mut model = Model::new();
        fill(&db, CHECKPOINT_INTERVAL, &mut model).await;
        db.cleanup(NOW).await.unwrap();
        assert!(log_files(&store).await.is_empty(), "{backend:?}");

        let v = db.put("k0", text("after")).await.unwrap();
        model.insert("k0".into(), (text("after"), v));
        assert_state(&open(&store).await, &model, &format!("{backend:?}")).await;
    }
}

#[tokio::test]
async fn retention_keeps_recent_checkpoints() {
    for backend in BACKENDS {
        let store = store(backend, "cleanup_retention");
        let db = open(&store).await;
        fill(&db, CHECKPOINT_INTERVAL + 10, &mut Model::new()).await;
        let before = log_files(&store).await;

        db.cleanup(HOUR).await.unwrap();
        assert_eq!(log_files(&store).await, before, "{backend:?}");
        assert_eq!(checkpoint_files(&store).await, names([CHECKPOINT_INTERVAL]));
    }
}

#[tokio::test]
async fn concurrent_cleanups_agree() {
    for backend in BACKENDS {
        let store = store(backend, "cleanup_concurrent");
        let mut model = Model::new();
        fill(&open(&store).await, 2 * CHECKPOINT_INTERVAL + 5, &mut model).await;

        let (a, b) = (open(&store).await, open(&store).await);
        let (ra, rb) = tokio::join!(a.cleanup(NOW), b.cleanup(NOW));
        ra.unwrap();
        rb.unwrap();
        assert_eq!(log_files(&store).await.len(), 5, "{backend:?}");
        assert_state(&open(&store).await, &model, &format!("{backend:?}")).await;
    }
}

/// A `Db` that was idle while its next commits were deleted moves to the newer checkpoint
/// instead of reporting a hole.
#[tokio::test]
async fn idle_db_catches_up_after_cleanup() {
    for backend in BACKENDS {
        let store = store(backend, "cleanup_idle");
        let writer = open(&store).await;
        let mut model = Model::new();
        fill(&writer, 30, &mut model).await;
        let idle = open(&store).await;
        assert_state(&idle, &model, "before").await;

        fill(&writer, 2 * CHECKPOINT_INTERVAL, &mut model).await;
        writer.cleanup(NOW).await.unwrap();
        assert_state(&idle, &model, &format!("{backend:?}")).await;
        idle.put("k0", text("idle")).await.unwrap();
    }
}

/// A transaction whose snapshot predates the cleaned-up range can neither read nor
/// commit: its reads cannot be checked any more.
#[tokio::test]
async fn transaction_older_than_cleanup_expires() {
    for backend in BACKENDS {
        let store = store(backend, "cleanup_tx");
        let db = open(&store).await;
        let mut model = Model::new();
        fill(&db, 30, &mut model).await;

        // A key nothing else writes, so only the expired check can stop the commit.
        let mut tx = db.begin();
        tx.get("untouched").await.unwrap();
        fill(&db, 2 * CHECKPOINT_INTERVAL, &mut model).await;
        db.cleanup(NOW).await.unwrap();

        let ctx = format!("{backend:?}");
        let read = tx.get("k1").await;
        assert!(
            matches!(read, Err(Error::Expired { .. })),
            "{ctx}: {read:?}"
        );
        tx.put("k2", text("late"));
        let result = tx.commit().await;
        assert!(
            matches!(result, Err(Error::Expired { .. })),
            "{ctx}: {result:?}"
        );
        assert_state(&db, &model, &ctx).await;
    }
}

/// A writer stalls between finding the head and its `Create`. Meanwhile the log moves on
/// and cleanup deletes the number it is about to create, so the `Create` succeeds, but no
/// reader would ever see it. The writer must report `Expired`, not success.
#[tokio::test]
async fn stalled_write_into_cleaned_up_range_is_expired() {
    for backend in BACKENDS {
        for prefill in [0, 10] {
            let inner = store(backend, &format!("cleanup_phantom_{prefill}"));
            let other = open(&inner).await;
            let mut model = Model::new();
            fill(&other, prefill, &mut model).await;

            let faulty = FaultStore::new(inner.clone());
            let stalled = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
            let paused = faulty.pause_next_create();
            let task = tokio::spawn(async move { stalled.put("k0", text("stalled")).await });
            paused.reached.await.unwrap();

            fill(&other, 2 * CHECKPOINT_INTERVAL, &mut model).await;
            other.cleanup(NOW).await.unwrap();
            // Store timestamps must tell the checkpoints from the re-created commit.
            tokio::time::sleep(Duration::from_millis(20)).await;
            paused.resume.send(()).unwrap();

            let ctx = format!("{backend:?}/prefill={prefill}");
            let result = task.await.unwrap();
            assert!(
                matches!(result, Err(Error::Expired { number }) if number == prefill + 1),
                "{ctx}: {result:?}"
            );
            assert_state(&open(&inner).await, &model, &ctx).await;
            // The orphan goes with the next cleanup.
            other.cleanup(NOW).await.unwrap();
            assert!(
                !log_files(&inner).await.contains(&commit_name(prefill + 1)),
                "{ctx}"
            );
        }
    }
}
