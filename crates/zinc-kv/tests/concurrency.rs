use std::sync::Arc;

use bytes::Bytes;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use zinc_kv::{CasError, Db};

async fn node(store: &Arc<dyn ObjectStore>) -> Db {
    Db::open(store.clone(), Path::from("mesh")).await.unwrap()
}

/// Several "nodes" (separate `Db`s on one store) increment a shared counter with
/// read + cas retry loops. Any lost update shows up as a total below the expected one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cas_increments_lose_nothing() {
    const NODES: usize = 8;
    const INCREMENTS: usize = 20;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let tasks: Vec<_> = (0..NODES)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move {
                let db = node(&store).await;
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

    let total = node(&store).await.get("counter").await.unwrap().unwrap();
    assert_eq!(total.value, Bytes::from((NODES * INCREMENTS).to_string()));
}

/// Leader election: many nodes race to create the same key; exactly one wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_one_node_wins_the_lock() {
    const NODES: usize = 16;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let tasks: Vec<_> = (0..NODES)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move {
                let db = node(&store).await;
                let id = Bytes::from(format!("node-{i}"));
                db.cas("leader", None, Some(id)).await.is_ok()
            })
        })
        .collect();

    let mut winners = 0;
    for t in tasks {
        winners += t.await.unwrap() as usize;
    }
    assert_eq!(winners, 1);
    assert!(node(&store).await.get("leader").await.unwrap().is_some());
}
