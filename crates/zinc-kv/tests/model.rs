//! Random sequences of operations through several `Db`s on one store, checked step by
//! step against a `HashMap`. Seeded, so a failure is reproducible from its seed.

mod common;

use std::collections::HashMap;

use bytes::Bytes;
use common::{open, store, Backend};
use rand::{rngs::StdRng, Rng, SeedableRng};
use zinc_kv::{CasError, Version};

const KEYS: [&str; 5] = ["a", "b", "c", "lock", "config/x"];
const NODES: usize = 3;

async fn run(backend: Backend, seed: u64, steps: usize) {
    let store = store(backend, &format!("model-{seed}"));
    let mut nodes = Vec::new();
    for _ in 0..NODES {
        nodes.push(open(&store).await);
    }
    let mut rng = StdRng::seed_from_u64(seed);
    let mut model: HashMap<&str, (Bytes, Version)> = HashMap::new();
    // Every version handed out, to build stale expectations and check monotonicity.
    let mut seen: Vec<Version> = Vec::new();

    for step in 0..steps {
        let ctx = format!("{backend:?} seed {seed} step {step}");
        let db = &nodes[rng.random_range(0..NODES)];
        let key = KEYS[rng.random_range(0..KEYS.len())];
        let value = Bytes::from(format!("{seed}-{step}"));
        let current = model.get(key).map(|(_, v)| *v);

        let new_version = match rng.random_range(0..5) {
            0 => {
                let got = db.get(key).await.unwrap().map(|e| (e.value, e.version));
                assert_eq!(got, model.get(key).cloned(), "{ctx}: get {key}");
                continue;
            }
            1 => {
                let v = db.put(key, value.clone()).await.unwrap();
                model.insert(key, (value, v));
                v
            }
            2 => {
                db.delete(key).await.unwrap();
                model.remove(key);
                continue;
            }
            3 => {
                // Correct expectation: must succeed. Half of them delete.
                let new = rng.random_bool(0.5).then(|| value.clone());
                let v = db
                    .cas(key, current, new.clone())
                    .await
                    .unwrap_or_else(|e| panic!("{ctx}: cas {key} {current:?}: {e}"));
                match new {
                    Some(value) => model.insert(key, (value, v)),
                    None => model.remove(key),
                };
                v
            }
            _ => {
                // Wrong expectation: a stale version, or "absent" for a live key.
                let wrong = match (current, seen.is_empty()) {
                    (Some(_), _) if rng.random_bool(0.3) => None,
                    (_, false) => Some(seen[rng.random_range(0..seen.len())]),
                    _ => continue,
                };
                if wrong == current {
                    continue;
                }
                match db.cas(key, wrong, Some(value)).await {
                    Err(CasError::Conflict { current: c }) => {
                        assert_eq!(c, current, "{ctx}: conflict on {key}")
                    }
                    other => panic!("{ctx}: cas {key} {wrong:?} expected conflict, got {other:?}"),
                }
                continue;
            }
        };
        if let Some(&last) = seen.last() {
            assert!(new_version > last, "{ctx}: {new_version:?} after {last:?}");
        }
        seen.push(new_version);
    }

    // Final state, read by every node.
    for db in &nodes {
        for key in KEYS {
            let got = db.get(key).await.unwrap().map(|e| (e.value, e.version));
            assert_eq!(got, model.get(key).cloned(), "{backend:?} seed {seed}: final {key}");
        }
    }
}

#[tokio::test]
async fn matches_model_in_memory() {
    for seed in 0..8 {
        run(Backend::Memory, seed, 400).await;
    }
}

#[tokio::test]
async fn matches_model_on_disk() {
    run(Backend::Disk, 100, 200).await;
}
