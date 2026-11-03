//! Request-count benchmark: runs typical workloads against a counting store and prints
//! the requests per operation, with the estimated cost and latency on S3.
//!
//! `cargo test --test bench -- --nocapture` to see the table.

mod common;

use std::{sync::Arc, time::Duration};

use common::{
    counting::{Calls, CountingStore, S3},
    open, text,
};
use object_store::{memory::InMemory, ObjectStore};
use zinc_kv::{Db, Error};

/// Commits in the store before each workload: past one checkpoint, with commits after it.
const HISTORY: u64 = 150;

struct Row {
    name: &'static str,
    ops: u64,
    calls: Calls,
}

/// A store holding `HISTORY` commits over a few keys, behind a counter.
async fn seeded() -> Arc<CountingStore> {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(&inner).await;
    for i in 0..HISTORY {
        let key = if i % 3 == 0 {
            format!("cfg/{}", i % 5)
        } else {
            format!("k{}", i % 7)
        };
        db.put(&key, text(&i.to_string())).await.unwrap();
    }
    CountingStore::new(inner)
}

fn as_store(counting: &Arc<CountingStore>) -> Arc<dyn ObjectStore> {
    counting.clone()
}

/// A `Db` that has already read once, as on a running node.
async fn warm(counting: &Arc<CountingStore>) -> Db {
    let db = open(&as_store(counting)).await;
    db.get("k0").await.unwrap();
    db
}

async fn cold_get() -> Row {
    let counting = seeded().await;
    let db = open(&as_store(&counting)).await;
    db.get("k1").await.unwrap();
    Row {
        name: "get, fresh Db",
        ops: 1,
        calls: counting.calls(),
    }
}

async fn poll_get() -> Row {
    let counting = seeded().await;
    let db = warm(&counting).await;
    let before = counting.calls();
    for _ in 0..100 {
        db.get("k1").await.unwrap();
    }
    Row {
        name: "get, polling, no changes",
        ops: 100,
        calls: counting.calls() - before,
    }
}

async fn missing_get() -> Row {
    let counting = seeded().await;
    let db = warm(&counting).await;
    let before = counting.calls();
    for _ in 0..100 {
        db.get("absent").await.unwrap();
    }
    Row {
        name: "get missing key, polling",
        ops: 100,
        calls: counting.calls() - before,
    }
}

async fn put() -> Row {
    let counting = seeded().await;
    let db = warm(&counting).await;
    let before = counting.calls();
    // Stays below the next checkpoint.
    for i in 0..40 {
        db.put("k2", text(&i.to_string())).await.unwrap();
    }
    Row {
        name: "put",
        ops: 40,
        calls: counting.calls() - before,
    }
}

async fn put_through_checkpoint() -> Row {
    let counting = seeded().await;
    let db = warm(&counting).await;
    let before = counting.calls();
    for i in 0..100 {
        db.put("k2", text(&i.to_string())).await.unwrap();
    }
    Row {
        name: "put, incl. one checkpoint per 100",
        ops: 100,
        calls: counting.calls() - before,
    }
}

async fn read_modify_write() -> Row {
    let counting = seeded().await;
    let db = warm(&counting).await;
    let before = counting.calls();
    for i in 0..40 {
        let mut tx = db.begin();
        tx.get("k3").await.unwrap();
        tx.put("k3", text(&i.to_string()));
        tx.commit().await.unwrap();
    }
    Row {
        name: "transaction: get + put",
        ops: 40,
        calls: counting.calls() - before,
    }
}

async fn list_prefix() -> Row {
    let counting = seeded().await;
    let db = warm(&counting).await;
    let before = counting.calls();
    for _ in 0..100 {
        db.list("cfg/").await.unwrap();
    }
    Row {
        name: "list prefix, polling",
        ops: 100,
        calls: counting.calls() - before,
    }
}

/// Four warm nodes try to take an empty lock at once; one attempt each.
async fn election() -> Row {
    const NODES: usize = 4;
    let counting = seeded().await;
    let mut nodes = Vec::new();
    for _ in 0..NODES {
        nodes.push(warm(&counting).await);
    }
    let before = counting.calls();
    let attempts = nodes.iter().enumerate().map(|(i, db)| async move {
        let mut tx = db.begin();
        if tx.get("leader").await.unwrap().is_some() {
            return false;
        }
        tx.put("leader", text(&format!("node-{i}")));
        match tx.commit().await {
            Ok(_) => true,
            Err(Error::Conflict { .. }) => false,
            Err(e) => panic!("{e}"),
        }
    });
    let won = futures::future::join_all(attempts).await;
    assert_eq!(won.iter().filter(|&&w| w).count(), 1);
    Row {
        name: "election attempt, 4 nodes",
        ops: NODES as u64,
        calls: counting.calls() - before,
    }
}

async fn cleanup() -> Row {
    let counting = seeded().await;
    let db = warm(&counting).await;
    let before = counting.calls();
    db.cleanup(Duration::ZERO).await.unwrap();
    Row {
        name: "cleanup (100 commits due)",
        ops: 1,
        calls: counting.calls() - before,
    }
}

#[tokio::test]
async fn request_counts() {
    let rows = vec![
        cold_get().await,
        poll_get().await,
        missing_get().await,
        put().await,
        put_through_checkpoint().await,
        read_modify_write().await,
        list_prefix().await,
        election().await,
        cleanup().await,
    ];

    println!(
        "\n{} model: {HISTORY} commits of history, calls per operation\n",
        S3.name
    );
    println!(
        "| {:<34} | {:>5} | {:>5} | {:>5} | {:>5} | {:>5} | {:>9} | {:>7} |",
        "workload", "GET", "HEAD", "PUT", "LIST", "DEL", "$/M ops", "ms/op"
    );
    println!(
        "|{:-<36}|{:->7}|{:->7}|{:->7}|{:->7}|{:->7}|{:->11}|{:->9}|",
        "", "", "", "", "", "", "", ""
    );
    for row in &rows {
        let per_op = row.calls.per_op(row.ops);
        let [get, head, put, list, delete] = per_op;
        println!(
            "| {:<34} | {get:>5.1} | {head:>5.1} | {put:>5.1} | {list:>5.1} | {delete:>5.1} | {:>9.2} | {:>7.0} |",
            row.name,
            S3.dollars(per_op),
            S3.millis(per_op),
        );
        assert!(row.calls.total() > 0, "{}: no requests counted", row.name);
    }
    println!(
        "\nLatency assumes {:?} ms per GET, HEAD, PUT, LIST, DELETE, all in sequence.",
        S3.millis
    );
}
