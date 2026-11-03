//! Checkpoints: reads start from the newest one, so their cost no longer grows with
//! history, and a checkpoint is never trusted beyond what its file proves.

mod common;

use std::{collections::HashMap, sync::Arc};

use common::{commit_name, disk, open, store, text, FaultStore, BACKENDS, PREFIX};
use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use zinc_kv::{Error, Version, CHECKPOINT_INTERVAL};

const KEYS: usize = 7;

/// Writes `commits` commits over a few keys (every fifth a delete) and returns the
/// expected state.
async fn fill(db: &zinc_kv::Db, commits: u64) -> HashMap<String, (bytes::Bytes, Version)> {
    let mut model = HashMap::new();
    for i in 0..commits {
        let key = format!("k{}", i as usize % KEYS);
        if i % 5 == 4 {
            db.delete(&key).await.unwrap();
            model.remove(&key);
        } else {
            let value = text(&i.to_string());
            let v = db.put(&key, value.clone()).await.unwrap();
            model.insert(key, (value, v));
        }
    }
    model
}

async fn assert_state(
    db: &zinc_kv::Db,
    model: &HashMap<String, (bytes::Bytes, Version)>,
    ctx: &str,
) {
    for k in 0..KEYS {
        let key = format!("k{k}");
        let got = db.get(&key).await.unwrap().map(|e| (e.value, e.version));
        assert_eq!(got, model.get(&key).cloned(), "{ctx}: {key}");
    }
    assert_eq!(db.get("never-written").await.unwrap(), None, "{ctx}");
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

fn checkpoint_path(number: u64) -> Path {
    Path::from(format!("{PREFIX}/checkpoints/{}", commit_name(number)))
}

/// Writers create a checkpoint at each interval; a fresh reader then fetches only the
/// commits after the newest one, even for a key that does not exist.
#[tokio::test]
async fn reads_start_from_the_newest_checkpoint() {
    for backend in BACKENDS {
        let inner = store(backend, "checkpoint_reads");
        let model = fill(&open(&inner).await, 2 * CHECKPOINT_INTERVAL + 50).await;
        assert_eq!(
            checkpoint_files(&inner).await,
            [
                commit_name(CHECKPOINT_INTERVAL),
                commit_name(2 * CHECKPOINT_INTERVAL)
            ],
            "{backend:?}"
        );

        let faulty = FaultStore::new(inner.clone());
        let reader = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        assert_eq!(reader.get("never-written").await.unwrap(), None);
        assert_eq!(faulty.commit_gets(), 50, "{backend:?}");
        // Cached: a second read fetches nothing new.
        assert_state(&reader, &model, &format!("{backend:?}")).await;
        assert_eq!(faulty.commit_gets(), 50, "{backend:?}");
    }
}

/// Commits before the newest checkpoint are not needed at all (as after cleanup).
#[tokio::test]
async fn commits_before_the_checkpoint_are_not_read() {
    let (store, root) = disk("checkpoint_without_old_commits");
    let store: Arc<dyn ObjectStore> = Arc::new(store);
    let model = fill(&open(&store).await, 2 * CHECKPOINT_INTERVAL + 10).await;
    for n in 1..=2 * CHECKPOINT_INTERVAL {
        std::fs::remove_file(root.join(PREFIX).join("log").join(commit_name(n))).unwrap();
    }

    let db = open(&store).await;
    assert_state(&db, &model, "after removing old commits").await;
    let mut tx = db.begin();
    tx.get("k0").await.unwrap();
    tx.put("k0", text("tx"));
    tx.commit().await.unwrap();
    db.put("k1", text("put")).await.unwrap();
}

/// Checkpoint writes fail for a while: commits still succeed, and once the store works
/// again the next commit writes every missing checkpoint, in order.
#[tokio::test]
async fn missed_checkpoints_are_caught_up() {
    for backend in BACKENDS {
        let inner = store(backend, "checkpoint_catch_up");
        let faulty = FaultStore::new(inner.clone());
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;

        faulty.fail_checkpoint_creates(true);
        let mut model = fill(&db, 2 * CHECKPOINT_INTERVAL + 5).await;
        assert!(checkpoint_files(&inner).await.is_empty(), "{backend:?}");

        faulty.fail_checkpoint_creates(false);
        let v = db.put("k0", text("last")).await.unwrap();
        model.insert("k0".into(), (text("last"), v));
        assert_eq!(
            checkpoint_files(&inner).await,
            [
                commit_name(CHECKPOINT_INTERVAL),
                commit_name(2 * CHECKPOINT_INTERVAL)
            ],
            "{backend:?}"
        );
        assert_state(&open(&inner).await, &model, &format!("{backend:?}")).await;
    }
}

/// A `Db` that only reads, opened before the checkpoints existed, moves to each new one as
/// the log reaches it instead of replaying from its old base.
#[tokio::test]
async fn long_lived_reader_follows_new_checkpoints() {
    for backend in BACKENDS {
        let inner = store(backend, "checkpoint_long_lived");
        let faulty = FaultStore::new(inner.clone());
        let reader = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        assert_eq!(reader.get("k0").await.unwrap(), None);

        let model = fill(&open(&inner).await, 2 * CHECKPOINT_INTERVAL + 30).await;
        assert_state(&reader, &model, &format!("{backend:?}")).await;
        assert_eq!(faulty.commit_gets(), 30, "{backend:?}");
    }
}

/// Losing the race to write a checkpoint is not a failure: the other node wrote the same
/// state.
#[tokio::test]
async fn checkpoint_written_by_another_node_is_accepted() {
    for backend in BACKENDS {
        let inner = store(backend, "checkpoint_race");
        let faulty = FaultStore::new(inner.clone());
        faulty.fail_checkpoint_creates(true);
        let db = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        let model = fill(&db, CHECKPOINT_INTERVAL + 1).await;

        faulty.fail_checkpoint_creates(false);
        faulty.checkpoint_already_exists(true);
        db.checkpoint().await.unwrap();
        assert_eq!(
            checkpoint_files(&inner).await,
            [commit_name(CHECKPOINT_INTERVAL)]
        );
        assert_state(&open(&inner).await, &model, &format!("{backend:?}")).await;
    }
}

/// Without any writer, `checkpoint()` writes what is missing; calling it again, or from a
/// second node, changes nothing.
#[tokio::test]
async fn explicit_checkpoint_is_idempotent() {
    for backend in BACKENDS {
        let inner = store(backend, "checkpoint_explicit");
        let faulty = FaultStore::new(inner.clone());
        faulty.fail_checkpoint_creates(true);
        let model = fill(
            &open(&(faulty as Arc<dyn ObjectStore>)).await,
            CHECKPOINT_INTERVAL + 1,
        )
        .await;
        assert!(checkpoint_files(&inner).await.is_empty(), "{backend:?}");

        let (a, b) = (open(&inner).await, open(&inner).await);
        a.checkpoint().await.unwrap();
        let first = inner
            .get(&checkpoint_path(CHECKPOINT_INTERVAL))
            .await
            .unwrap();
        let first = first.bytes().await.unwrap();
        a.checkpoint().await.unwrap();
        b.checkpoint().await.unwrap();
        assert_eq!(
            checkpoint_files(&inner).await,
            [commit_name(CHECKPOINT_INTERVAL)]
        );
        let again = inner
            .get(&checkpoint_path(CHECKPOINT_INTERVAL))
            .await
            .unwrap();
        assert_eq!(again.bytes().await.unwrap(), first, "{backend:?}");
        assert_state(&b, &model, &format!("{backend:?}")).await;
    }
}

/// A listing that lags hides the newest checkpoint: the reader probes past it and still
/// starts from the newest one.
#[tokio::test]
async fn lagging_checkpoint_listing_is_probed_past() {
    for backend in BACKENDS {
        let inner = store(backend, "checkpoint_lagging_listing");
        let model = fill(&open(&inner).await, 2 * CHECKPOINT_INTERVAL + 20).await;

        let faulty = FaultStore::new(inner.clone());
        faulty.cap_listing_at(CHECKPOINT_INTERVAL + 50);
        let reader = open(&(faulty.clone() as Arc<dyn ObjectStore>)).await;
        assert_state(&reader, &model, &format!("{backend:?}")).await;
        assert_eq!(faulty.commit_gets(), 20, "{backend:?}");
    }
}

/// A transaction keeps reading at its snapshot after the `Db` moves to a newer
/// checkpoint, and its commit still checks every commit after the snapshot.
#[tokio::test]
async fn transaction_snapshot_survives_a_new_checkpoint() {
    for backend in BACKENDS {
        let inner = store(backend, "checkpoint_tx");
        let db = open(&inner).await;
        fill(&db, CHECKPOINT_INTERVAL - 5).await;
        db.put("k3", text("kept")).await.unwrap();
        let before = db.get("k3").await.unwrap();

        for touched in [false, true] {
            let mut tx = db.begin();
            tx.get("k0").await.unwrap();
            // Cross a checkpoint, writing k1 (and k0 when `touched`).
            for i in 0..CHECKPOINT_INTERVAL {
                db.put("k1", text(&format!("new-{i}"))).await.unwrap();
            }
            if touched {
                db.put("k0", text("changed")).await.unwrap();
            }
            // Read at the snapshot, from commits the `Db` has since moved past.
            assert_eq!(tx.get("k3").await.unwrap(), before, "{backend:?}");
            tx.put("k2", text("tx"));
            let result = tx.commit().await;
            let ctx = format!("{backend:?}/touched={touched}");
            if touched {
                assert!(
                    matches!(result, Err(Error::Conflict { ref key }) if key == "k0"),
                    "{ctx}: {result:?}"
                );
            } else {
                result.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn damaged_checkpoints_are_errors() {
    for backend in BACKENDS {
        for (label, contents) in [
            ("not_json", "not json".to_owned()),
            ("unknown_field", r#"{"v":1,"entries":[],"extra":1}"#.to_owned()),
            (
                "version_from_the_future",
                format!(r#"{{"v":1,"entries":[{{"key":"k","value":"","version":{}}}]}}"#, CHECKPOINT_INTERVAL + 1),
            ),
            (
                "unsorted",
                r#"{"v":1,"entries":[{"key":"b","value":"","version":1},{"key":"a","value":"","version":1}]}"#.to_owned(),
            ),
            ("future_format", r#"{"v":2,"anything":true}"#.to_owned()),
        ] {
            let inner = store(backend, &format!("checkpoint_damaged_{label}"));
            fill(&open(&inner).await, CHECKPOINT_INTERVAL + 1).await;
            inner
                .put_opts(&checkpoint_path(CHECKPOINT_INTERVAL), text(&contents).into(), Default::default())
                .await
                .unwrap();

            let db = open(&inner).await;
            let ctx = format!("{backend:?}/{label}");
            for result in [db.get("k0").await.map(|_| ()), db.put("k0", text("x")).await.map(|_| ())] {
                match label {
                    "future_format" => assert!(
                        matches!(result, Err(Error::UnsupportedCheckpointFormat { number, version: 2 }) if number == CHECKPOINT_INTERVAL),
                        "{ctx}: {result:?}"
                    ),
                    _ => assert!(
                        matches!(result, Err(Error::CorruptCheckpoint { number, .. }) if number == CHECKPOINT_INTERVAL),
                        "{ctx}: {result:?}"
                    ),
                }
            }
        }
    }
}
