mod common;

use std::collections::HashMap;

use bytes::Bytes;
use common::{open, store, text, BACKENDS};
use zinc_kv::CasError;

/// Several "nodes" (separate `Db`s on one store) increment a shared counter with
/// read + cas retry loops. Any lost update shows up as a total below the expected one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cas_increments_lose_nothing() {
    const NODES: usize = 8;
    const INCREMENTS: usize = 20;

    for backend in BACKENDS {
        let store = store(backend, "counter");

        let tasks: Vec<_> = (0..NODES)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move {
                    let db = open(&store).await;
                    for _ in 0..INCREMENTS {
                        loop {
                            let current = db.get("counter").await.unwrap();
                            let (expected, n) = match &current {
                                Some(e) => (Some(e.version), std::str::from_utf8(&e.value).unwrap().parse::<u64>().unwrap()),
                                None => (None, 0),
                            };
                            match db.cas("counter", expected, Some(Bytes::from((n + 1).to_string()))).await {
                                Ok(_) => break,
                                Err(CasError::Conflict { .. }) => continue,
                                Err(e) => panic!("{e}"),
                            }
                        }
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }

        let total = open(&store).await.get("counter").await.unwrap().unwrap();
        assert_eq!(total.value, Bytes::from((NODES * INCREMENTS).to_string()), "{backend:?}");
    }
}

/// Leader election: many nodes race to create the same key; exactly one wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_one_node_wins_the_lock() {
    const NODES: usize = 16;

    for backend in BACKENDS {
        let store = store(backend, "lock");

        let tasks: Vec<_> = (0..NODES)
            .map(|i| {
                let store = store.clone();
                tokio::spawn(async move {
                    let db = open(&store).await;
                    let id = Bytes::from(format!("node-{i}"));
                    match db.cas("leader", None, Some(id.clone())).await {
                        Ok(_) => Some(id),
                        Err(CasError::Conflict { .. }) => None,
                        Err(e) => panic!("{e}"),
                    }
                })
            })
            .collect();

        let mut winners = Vec::new();
        for t in tasks {
            winners.extend(t.await.unwrap());
        }
        assert_eq!(winners.len(), 1, "{backend:?}");
        let leader = open(&store).await.get("leader").await.unwrap().unwrap();
        assert_eq!(leader.value, winners[0], "{backend:?}");
    }
}

/// Writers on disjoint keys with puts and deletes interleaved. Each writer's own sequence
/// must be preserved: strictly increasing versions per key, and the final value of every
/// key is the last thing its writer did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_workload_keeps_every_last_write() {
    const WRITERS: usize = 6;
    const KEYS: usize = 3;
    const ROUNDS: usize = 8;

    for backend in BACKENDS {
        let store = store(backend, "mixed");

        let tasks: Vec<_> = (0..WRITERS)
            .map(|w| {
                let store = store.clone();
                tokio::spawn(async move {
                    let db = open(&store).await;
                    let mut last_version = HashMap::new();
                    for round in 0..ROUNDS {
                        for k in 0..KEYS {
                            let key = format!("w{w}-k{k}");
                            if round % 3 == 2 {
                                db.delete(&key).await.unwrap();
                                assert_eq!(db.get(&key).await.unwrap(), None);
                            } else {
                                let v = db.put(&key, text(&format!("{round}"))).await.unwrap();
                                if let Some(prev) = last_version.insert(key.clone(), v) {
                                    assert!(v > prev, "{key}: {v:?} after {prev:?}");
                                }
                            }
                        }
                    }
                    // Key 0 ends deleted, the others at the last round's value.
                    db.delete(&format!("w{w}-k0")).await.unwrap();
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }

        let db = open(&store).await;
        for w in 0..WRITERS {
            assert_eq!(db.get(&format!("w{w}-k0")).await.unwrap(), None, "{backend:?}");
            for k in 1..KEYS {
                let entry = db.get(&format!("w{w}-k{k}")).await.unwrap().unwrap();
                assert_eq!(entry.value, text(&format!("{}", ROUNDS - 1)), "{backend:?}");
            }
        }
    }
}
