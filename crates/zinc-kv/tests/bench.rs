//! Cluster simulation: several nodes share one store and run a coordination loop
//! concurrently for a while; afterwards we count what the requests would have cost on S3.
//!
//! `cargo test --release --test bench -- --nocapture` to see the report.

mod common;

use std::sync::Arc;

use common::{
    counting::{Calls, CountingStore, S3},
    text,
};
use object_store::{memory::InMemory, path::Path, ObjectStore};
use zinc_kv::{Db, Error};

const NODES: usize = 8;
const ROUNDS: usize = 200;
/// A node writes its heartbeat every this many rounds.
const HEARTBEAT_EVERY: usize = 10;

/// What one node got done.
#[derive(Default)]
struct Work {
    /// Rounds in which this node held the lease (and renewed it).
    led: usize,
    /// Lease attempts lost to another node's commit.
    conflicts: usize,
    heartbeats: usize,
}

/// One node's loop. Each round: read the shared config, then take or renew the leader
/// lease if it is free or ours; every few rounds, write a heartbeat.
async fn node(id: usize, db: Db) -> Work {
    let me = text(&format!("node-{id}"));
    let mut work = Work::default();
    for round in 0..ROUNDS {
        db.list("config/").await.unwrap();

        let mut tx = db.begin();
        let leader = tx.get("leader").await.unwrap();
        if leader.as_ref().is_none_or(|e| e.value == me) {
            tx.put("leader", me.clone());
            match tx.commit().await {
                Ok(_) => work.led += 1,
                Err(Error::Conflict { .. }) => work.conflicts += 1,
                Err(e) => panic!("node {id}: {e}"),
            }
        }

        if round % HEARTBEAT_EVERY == 0 {
            db.put(&format!("nodes/{id}"), text(&round.to_string()))
                .await
                .unwrap();
            work.heartbeats += 1;
        }
    }
    work
}

fn report_line(name: &str, rounds: usize, calls: Calls) {
    let per_round = calls.per_op(rounds as u64);
    let [get, head, put, list, delete] = per_round;
    let total = calls.per_op(1);
    println!(
        "| {name:<8} | {get:>5.2} | {head:>5.2} | {put:>5.2} | {list:>5.2} | {delete:>5.2} | {:>9.4} | {:>8.1} |",
        S3.dollars(total) / 1e6 * 1e3,
        S3.millis(total) / 1e3,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_simulation() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seed = Db::open(inner.clone(), Path::from("mesh")).await.unwrap();
    for key in ["config/region", "config/replicas", "config/flags"] {
        seed.put(key, text("initial")).await.unwrap();
    }

    let counters: Vec<_> = (0..NODES)
        .map(|_| CountingStore::new(inner.clone()))
        .collect();
    let tasks: Vec<_> = counters
        .iter()
        .enumerate()
        .map(|(id, counter)| {
            let store: Arc<dyn ObjectStore> = counter.clone();
            tokio::spawn(async move {
                let db = Db::open(store, Path::from("mesh")).await.unwrap();
                node(id, db).await
            })
        })
        .collect();
    let mut work = Vec::new();
    for task in tasks {
        work.push(task.await.unwrap());
    }

    let led: usize = work.iter().map(|w| w.led).sum();
    let conflicts: usize = work.iter().map(|w| w.conflicts).sum();
    let heartbeats: usize = work.iter().map(|w| w.heartbeats).sum();
    // Exactly one lease renewal per round can win, but rounds are not synchronised
    // between nodes, so only the totals are checked.
    assert!(led > 0 && heartbeats == NODES * ROUNDS / HEARTBEAT_EVERY);

    println!(
        "\n{NODES} nodes x {ROUNDS} rounds on {}: list config, take/renew lease, heartbeat every {HEARTBEAT_EVERY}",
        S3.name
    );
    println!("work: {led} lease renewals, {conflicts} lost lease races, {heartbeats} heartbeats\n");
    println!(
        "| {:<8} | {:>5} | {:>5} | {:>5} | {:>5} | {:>5} | {:>9} | {:>8} |",
        "node", "GET", "HEAD", "PUT", "LIST", "DEL", "m$ total", "busy s"
    );
    println!(
        "|{:-<10}|{:->7}|{:->7}|{:->7}|{:->7}|{:->7}|{:->11}|{:->10}|",
        "", "", "", "", "", "", "", ""
    );
    let mut total = Calls::default();
    let mut slowest = 0.0f64;
    for (id, counter) in counters.iter().enumerate() {
        let calls = counter.calls();
        report_line(&format!("node {id}"), ROUNDS, calls);
        total = total + calls;
        slowest = slowest.max(S3.millis(calls.per_op(1)));
    }
    report_line("all", ROUNDS * NODES, total);
    println!(
        "\nColumns: requests per round; total cost in thousandths of a dollar; busy time if every \
         request took {:?} ms (GET, HEAD, PUT, LIST, DELETE) in sequence.",
        S3.millis
    );
    println!(
        "Cluster: {} requests, ${:.4} total, ${:.2} per million node-rounds, {:.1} s elapsed (slowest node).",
        total.total(),
        S3.dollars(total.per_op(1)) / 1e6,
        S3.dollars(total.per_op((ROUNDS * NODES) as u64)),
        slowest / 1e3,
    );
}
