//! `list(prefix)`: the newest state of every key under a prefix, merged from the checkpoint
//! and the commits after it, and in a transaction a dependency on the whole prefix.

mod common;

use bytes::Bytes;
use common::{open, store, text, BACKENDS};
use zinc_kv::{Entry, Error, CHECKPOINT_INTERVAL};

fn values(entries: &[(String, Entry)]) -> Vec<(&str, Bytes)> {
    entries
        .iter()
        .map(|(k, e)| (k.as_str(), e.value.clone()))
        .collect()
}

/// Keys under the prefix only, sorted, deleted ones left out, values and versions from
/// whichever of the checkpoint and the later commits is newer.
#[tokio::test]
async fn list_merges_checkpoint_and_commits() {
    for backend in BACKENDS {
        let store = store(backend, "list_merge");
        let db = open(&store).await;
        let v_b = db.put("cfg/b", text("old")).await.unwrap();
        db.put("cfg/gone", text("x")).await.unwrap();
        db.put("cfgx", text("not under cfg/")).await.unwrap();
        db.put("other", text("y")).await.unwrap();
        for i in 0..CHECKPOINT_INTERVAL {
            db.put("filler", text(&i.to_string())).await.unwrap();
        }
        // After the checkpoint: one update, one delete, one new key.
        db.put("cfg/a", text("new")).await.unwrap();
        db.delete("cfg/gone").await.unwrap();
        let v_c = db.put("cfg/c", text("c")).await.unwrap();

        let ctx = format!("{backend:?}");
        let listed = db.list("cfg/").await.unwrap();
        assert_eq!(
            values(&listed),
            [
                ("cfg/a", text("new")),
                ("cfg/b", text("old")),
                ("cfg/c", text("c"))
            ],
            "{ctx}"
        );
        assert_eq!(listed[1].1.version, v_b, "{ctx}");
        assert_eq!(listed[2].1.version, v_c, "{ctx}");

        let all = open(&store).await.list("").await.unwrap();
        let keys: Vec<&str> = all.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            ["cfg/a", "cfg/b", "cfg/c", "cfgx", "filler", "other"],
            "{ctx}"
        );
        assert!(db.list("nothing/").await.unwrap().is_empty(), "{ctx}");
    }
}

/// The last op on a key within one commit decides it.
#[tokio::test]
async fn list_honours_the_last_op_in_a_commit() {
    let store = store(common::Backend::Memory, "list_last_op");
    let db = open(&store).await;
    let mut tx = db.begin();
    tx.put("p/a", text("first"));
    tx.delete("p/a");
    tx.put("p/b", text("first"));
    tx.put("p/b", text("second"));
    tx.commit().await.unwrap();
    assert_eq!(
        values(&db.list("p/").await.unwrap()),
        [("p/b", text("second"))]
    );
}

/// In a transaction, a listed prefix is a dependency: creating, changing or deleting any
/// key under it conflicts; writes elsewhere do not.
#[tokio::test]
async fn listed_prefix_conflicts_on_any_change_under_it() {
    for backend in BACKENDS {
        let store = store(backend, "list_conflict");
        let db = open(&store).await;
        db.put("members/a", text("a")).await.unwrap();

        for (label, key, delete) in [
            ("create", "members/new", false),
            ("update", "members/a", false),
            ("delete", "members/a", true),
        ] {
            let mut tx = db.begin();
            let members = tx.list("members/").await.unwrap();
            if delete {
                db.delete(key).await.unwrap();
            } else {
                db.put(key, text(label)).await.unwrap();
            }
            tx.put("count", text(&members.len().to_string()));
            let result = tx.commit().await;
            assert!(
                matches!(&result, Err(Error::Conflict { key: k }) if k == key),
                "{backend:?}/{label}: {result:?}"
            );
            // Restore for the next case.
            db.put("members/a", text("a")).await.unwrap();
        }

        let mut tx = db.begin();
        tx.list("members/").await.unwrap();
        db.put("membersx", text("not under the prefix"))
            .await
            .unwrap();
        tx.put("count", text("1"));
        tx.commit().await.unwrap();
    }
}

#[tokio::test]
async fn listing_a_prefix_already_written_is_an_error() {
    let store = store(common::Backend::Memory, "list_after_write");
    let db = open(&store).await;
    let mut tx = db.begin();
    tx.put("members/me", text("1"));
    let result = tx.list("members/").await;
    assert!(
        matches!(&result, Err(Error::ReadAfterWrite { key }) if key == "members/me"),
        "{result:?}"
    );
    // A prefix that does not cover the write is fine.
    tx.list("other/").await.unwrap();
}

/// A transaction's list and gets see one snapshot.
#[tokio::test]
async fn list_reads_at_the_snapshot() {
    for backend in BACKENDS {
        let store = store(backend, "list_snapshot");
        let db = open(&store).await;
        db.put("s/a", text("1")).await.unwrap();

        let mut tx = db.begin();
        tx.get("unrelated").await.unwrap();
        db.put("s/b", text("2")).await.unwrap();
        assert_eq!(
            values(&tx.list("s/").await.unwrap()),
            [("s/a", text("1"))],
            "{backend:?}"
        );
    }
}
