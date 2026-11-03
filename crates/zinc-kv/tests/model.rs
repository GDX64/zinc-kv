//! Random sequences of operations through several `Db`s on one store, checked step by
//! step against a `HashMap`. Seeded, so a failure is reproducible from its seed.

mod common;

use std::collections::HashMap;

use bytes::Bytes;
use common::{open, store, Backend};
use rand::{rngs::StdRng, Rng, SeedableRng};
use zinc_kv::{Error, Version};

const KEYS: [&str; 5] = ["a", "b", "c", "lock", "config/x"];
const NODES: usize = 3;
const PREFIXES: [&str; 5] = ["", "c", "config/", "l", "z"];

type Model = HashMap<&'static str, (Bytes, Version)>;

fn listed(model: &Model, prefix: &str) -> Vec<(String, Bytes, Version)> {
    let mut entries: Vec<_> = model
        .iter()
        .filter(|(k, _)| k.starts_with(prefix))
        .map(|(k, (value, version))| (k.to_string(), value.clone(), *version))
        .collect();
    entries.sort();
    entries
}

fn flatten(entries: Vec<(String, zinc_kv::Entry)>) -> Vec<(String, Bytes, Version)> {
    entries
        .into_iter()
        .map(|(k, e)| (k, e.value, e.version))
        .collect()
}

async fn run(backend: Backend, seed: u64, steps: usize) {
    let store = store(backend, &format!("model-{seed}"));
    let mut nodes = Vec::new();
    for _ in 0..NODES {
        nodes.push(open(&store).await);
    }
    let mut rng = StdRng::seed_from_u64(seed);
    let mut model = Model::new();
    // Every version handed out must exceed the one before.
    let mut last: Option<Version> = None;
    let mut record = |v: Version, ctx: &str| {
        if let Some(prev) = last {
            assert!(v > prev, "{ctx}: {v:?} after {prev:?}");
        }
        last = Some(v);
    };

    for step in 0..steps {
        let ctx = format!("{backend:?} seed {seed} step {step}");
        let db = &nodes[rng.random_range(0..NODES)];
        let key = KEYS[rng.random_range(0..KEYS.len())];
        let value = Bytes::from(format!("{seed}-{step}"));

        match rng.random_range(0..5) {
            0 if rng.random_bool(0.5) => {
                let prefix = PREFIXES[rng.random_range(0..PREFIXES.len())];
                let got = flatten(db.list(prefix).await.unwrap());
                assert_eq!(got, listed(&model, prefix), "{ctx}: list {prefix:?}");
            }
            0 => {
                let got = db.get(key).await.unwrap().map(|e| (e.value, e.version));
                assert_eq!(got, model.get(key).cloned(), "{ctx}: get {key}");
            }
            1 => {
                let v = db.put(key, value.clone()).await.unwrap();
                record(v, &ctx);
                model.insert(key, (value, v));
            }
            2 => {
                db.delete(key).await.unwrap();
                model.remove(key);
            }
            op => {
                // A transaction reading `key`, maybe one more key and maybe a prefix, then
                // writing `key` (half of them delete). In op 4 another node writes a random
                // key between the reads and the commit: a conflict exactly when it was read
                // or lies under the listed prefix.
                let mut tx = db.begin();
                let mut reads = vec![key];
                if rng.random_bool(0.5) {
                    reads.push(KEYS[rng.random_range(0..KEYS.len())]);
                }
                for &k in &reads {
                    let got = tx.get(k).await.unwrap().map(|e| (e.value, e.version));
                    assert_eq!(got, model.get(k).cloned(), "{ctx}: tx get {k}");
                }
                let prefix = rng
                    .random_bool(0.3)
                    .then(|| PREFIXES[rng.random_range(0..PREFIXES.len())]);
                if let Some(prefix) = prefix {
                    let got = flatten(tx.list(prefix).await.unwrap());
                    assert_eq!(got, listed(&model, prefix), "{ctx}: tx list {prefix:?}");
                }
                let interfering = if op == 4 {
                    let other = KEYS[rng.random_range(0..KEYS.len())];
                    let other_value = Bytes::from(format!("{seed}-{step}-other"));
                    let v = nodes[rng.random_range(0..NODES)]
                        .put(other, other_value.clone())
                        .await
                        .unwrap();
                    record(v, &ctx);
                    model.insert(other, (other_value, v));
                    Some(other)
                } else {
                    None
                };
                let new = rng.random_bool(0.5).then(|| value.clone());
                match &new {
                    Some(value) => tx.put(key, value.clone()),
                    None => tx.delete(key),
                }
                let result = tx.commit().await;
                let depends = |k: &&str| reads.contains(k) || prefix.is_some_and(|p| k.starts_with(p));
                match interfering.filter(depends) {
                    Some(changed) => match result {
                        Err(Error::Conflict { key: k }) => assert_eq!(k, changed, "{ctx}"),
                        other => panic!("{ctx}: reads {reads:?} {prefix:?}, {changed} changed, got {other:?}"),
                    },
                    None => {
                        let v = result
                            .unwrap_or_else(|e| panic!("{ctx}: tx on {reads:?}: {e}"))
                            .epoch
                            .unwrap();
                        record(v, &ctx);
                        match new {
                            Some(value) => model.insert(key, (value, v)),
                            None => model.remove(key),
                        };
                    }
                }
            }
        }
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
