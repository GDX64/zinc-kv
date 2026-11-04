//! Timeline: N nodes create, update, delete and read random keys concurrently while a
//! janitor runs cleanup. A sampler records requests, estimated cost, storage and outcomes
//! as the run goes. Runs once per cluster size and request delay.
//!
//! `cargo test --release --test timeline -- --ignored --nocapture`
//! writes `target/tmp/timeline-<n>-nodes-<delay>ms.csv` per scenario and one chart with all of them,
//! `target/tmp/timeline.html` (paths printed at the end).

mod common;

use std::{
    fmt::Write as _,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use common::counting::{Calls, CountingStore, S3};
use futures::TryStreamExt;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use rand::{rngs::StdRng, Rng, SeedableRng};
use zinc_kv::{Db, Error};

/// Cluster sizes to run, one after another, each without and with a delay added to every
/// store request (like a remote store's round trip).
const NODE_COUNTS: [usize; 3] = [1, 8, 16];
const REQUEST_DELAYS: [Duration; 1] = [Duration::from_millis(0)];
const OPS_PER_NODE: u64 = 1000;
/// Keys are `data/000` .. `data/199`.
const KEYS: usize = 200;
const VALUE_BYTES: std::ops::Range<usize> = 16..1024;
/// Samples per run, whatever its length.
const SAMPLES: u64 = 100;
/// Cluster operations between cleanups.
const CLEANUP_EVERY: u64 = 1000;
/// Short, since the whole run takes seconds; operations stalled longer get `Expired`.
const RETENTION: Duration = Duration::from_millis(200);

/// Counters shared by every task.
#[derive(Default)]
struct Shared {
    ops: AtomicU64,
    gets: AtomicU64,
    lists: AtomicU64,
    creates: AtomicU64,
    updates: AtomicU64,
    deletes: AtomicU64,
    conflicts: AtomicU64,
    expired: AtomicU64,
    contention: AtomicU64,
    cleanups: AtomicU64,
    done: AtomicBool,
}

fn value(rng: &mut StdRng) -> Bytes {
    Bytes::from(vec![b'x'; rng.random_range(VALUE_BYTES)])
}

/// One operation picked at random; returns the counter for what it accomplished.
async fn operation<'a>(
    db: &Db,
    rng: &mut StdRng,
    shared: &'a Shared,
) -> Result<&'a AtomicU64, Error> {
    let key = format!("data/{:03}", rng.random_range(0..KEYS));
    Ok(match rng.random_range(0..100) {
        0..40 => {
            db.get(&key).await?;
            &shared.gets
        }
        40..45 => {
            db.list("data/01").await?;
            &shared.lists
        }
        // Create if absent, update if present: read, then write in one transaction.
        45..90 => {
            let mut tx = db.begin();
            let existed = tx.get(&key).await?.is_some();
            tx.put(&key, value(rng));
            tx.commit().await?;
            if existed {
                &shared.updates
            } else {
                &shared.creates
            }
        }
        _ => {
            db.delete(&key).await?;
            &shared.deletes
        }
    })
}

async fn node(id: usize, db: Db, shared: Arc<Shared>) {
    let mut rng = StdRng::seed_from_u64(id as u64);
    for _ in 0..OPS_PER_NODE {
        let counter = match operation(&db, &mut rng, &shared).await {
            Ok(counter) => counter,
            Err(Error::Conflict { .. }) => &shared.conflicts,
            Err(Error::Expired { .. }) => &shared.expired,
            Err(Error::Contention) => &shared.contention,
            Err(e) => panic!("node {id}: {e}"),
        };
        counter.fetch_add(1, Relaxed);
        shared.ops.fetch_add(1, Relaxed);
    }
}

async fn janitor(db: Db, shared: Arc<Shared>) {
    let mut next = CLEANUP_EVERY;
    while !shared.done.load(Relaxed) {
        if shared.ops.load(Relaxed) >= next {
            match db.cleanup(RETENTION).await {
                Ok(()) => shared.cleanups.fetch_add(1, Relaxed),
                Err(Error::Contention) => shared.contention.fetch_add(1, Relaxed),
                Err(e) => panic!("cleanup: {e}"),
            };
            next += CLEANUP_EVERY;
        }
        // Not a timer sleep: on Windows that rounds up to about 15 ms.
        tokio::task::yield_now().await;
    }
}

/// Objects and bytes under `dir`, measured on the inner store so it is not counted.
async fn usage(store: &Arc<dyn ObjectStore>, dir: &str) -> (u64, u64) {
    let objects: Vec<_> = store
        .list(Some(&Path::from(format!("mesh/{dir}"))))
        .try_collect()
        .await
        .unwrap();
    (objects.len() as u64, objects.iter().map(|o| o.size).sum())
}

const HEADER: &str = "ops,elapsed_ms,get,head,put,list,delete,requests,cost_usd,est_busy_s,\
log_objects,log_bytes,checkpoint_objects,checkpoint_bytes,storage_bytes,live_keys,live_bytes,\
gets,lists,creates,updates,deletes,conflicts,expired,contention,cleanups";

async fn sample(
    started: Instant,
    counters: &[Arc<CountingStore>],
    inner: &Arc<dyn ObjectStore>,
    observer: &Db,
    shared: &Shared,
) -> String {
    let calls = counters
        .iter()
        .fold(Calls::default(), |sum, c| sum + c.calls());
    // Nodes run in parallel: the cluster is as busy as its busiest node.
    let busy_ms = counters
        .iter()
        .map(|c| S3.millis(c.calls().per_op(1)))
        .fold(0.0, f64::max);
    let (log_objects, log_bytes) = usage(inner, "log").await;
    let (checkpoint_objects, checkpoint_bytes) = usage(inner, "checkpoints").await;
    let live = observer.list("data/").await.unwrap();
    let live_bytes: usize = live.iter().map(|(k, e)| k.len() + e.value.len()).sum();
    let n = |c: &AtomicU64| c.load(Relaxed);
    format!(
        "{},{},{},{},{},{},{},{},{:.6},{:.1},{log_objects},{log_bytes},{checkpoint_objects},\
         {checkpoint_bytes},{},{},{live_bytes},{},{},{},{},{},{},{},{},{}",
        n(&shared.ops),
        started.elapsed().as_millis(),
        calls.get,
        calls.head,
        calls.put,
        calls.list,
        calls.delete,
        calls.total(),
        S3.dollars(calls.per_op(1)) / 1e6,
        busy_ms / 1e3,
        log_bytes + checkpoint_bytes,
        live.len(),
        n(&shared.gets),
        n(&shared.lists),
        n(&shared.creates),
        n(&shared.updates),
        n(&shared.deletes),
        n(&shared.conflicts),
        n(&shared.expired),
        n(&shared.contention),
        n(&shared.cleanups),
    )
}

/// Runs `nodes` nodes and a janitor on a fresh store, every request delayed by `delay`;
/// returns the samples as CSV.
async fn run(nodes: usize, delay: Duration) -> String {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shared = Arc::new(Shared::default());
    // One counter per node plus the janitor, all on the same store.
    let counters: Vec<_> = (0..=nodes)
        .map(|_| CountingStore::with_delay(inner.clone(), delay))
        .collect();
    let open = |counter: &Arc<CountingStore>| {
        let store: Arc<dyn ObjectStore> = counter.clone();
        async move { Db::open(store, Path::from("mesh")).await.unwrap() }
    };
    // Reads for the report only; its requests are not counted.
    let observer = Db::open(inner.clone(), Path::from("mesh")).await.unwrap();

    let started = Instant::now();
    let mut tasks = Vec::new();
    for (id, counter) in counters[..nodes].iter().enumerate() {
        tasks.push(tokio::spawn(node(id, open(counter).await, shared.clone())));
    }
    let janitor = tokio::spawn(janitor(open(&counters[nodes]).await, shared.clone()));

    let mut csv = format!(
        "{HEADER}
"
    );
    let total = nodes as u64 * OPS_PER_NODE;
    let every = (total / SAMPLES).max(1);
    let mut next = 0;
    loop {
        let ops = shared.ops.load(Relaxed);
        if ops >= next || ops == total {
            let row = sample(started, &counters, &inner, &observer, &shared).await;
            writeln!(csv, "{row}").unwrap();
            if ops == total {
                break;
            }
            next = ops / every * every + every;
        }
        // Not a timer sleep: on Windows that rounds up to about 15 ms.
        tokio::task::yield_now().await;
    }
    for task in tasks {
        task.await.unwrap();
    }
    shared.done.store(true, Relaxed);
    janitor.await.unwrap();
    csv
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark: run with --ignored --nocapture"]
async fn storage_and_requests_over_time() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let mut scenarios = Vec::new();
    println!();
    for delay in REQUEST_DELAYS {
        for nodes in NODE_COUNTS {
            let csv = run(nodes, delay).await;
            let ms = delay.as_millis();
            let path = dir.join(format!("timeline-{nodes}-nodes-{ms}ms.csv"));
            std::fs::write(&path, &csv).unwrap();
            println!(
                "{nodes} nodes, {delay:?} per request: {} samples in {}",
                csv.lines().count() - 1,
                path.display()
            );
            println!("  {HEADER}\n  {}", csv.lines().last().unwrap());
            scenarios.push(serde_json::json!({
                "nodes": nodes,
                "delay": format!("{delay:?}"),
                "csv": csv,
            }));
        }
    }

    let data = serde_json::json!({
        "scenarios": scenarios,
        "ops_per_node": OPS_PER_NODE,
        "keys": KEYS,
        "cleanup_every": CLEANUP_EVERY,
        "retention": format!("{RETENTION:?}"),
        "model": S3.name,
    });
    let html = include_str!("timeline_chart.html").replace("__DATA__", &data.to_string());
    std::fs::write(dir.join("timeline.html"), html).unwrap();
    println!("Chart: {}", dir.join("timeline.html").display());
}
