use std::sync::Arc;

use bytes::Bytes;
use object_store::{memory::InMemory, path::Path};
use zinc_kv::{Db, Error};

#[tokio::test]
async fn transaction_put_get_delete() {
    let store = Arc::new(InMemory::new());
    let db = Db::open(store, Path::from("mesh")).await.unwrap();

    // Leader election. Commit succeeds only if nothing read has changed since.
    let mut tx = db.begin();
    assert!(tx.get("leader").await.unwrap().is_none());
    tx.put("leader", Bytes::from("node-a"));
    // The version of every key the commit wrote: the new leader's epoch.
    let epoch = tx.commit().await.unwrap().epoch.unwrap();

    // Read-modify-write across keys, applied together.
    let mut tx = db.begin();
    let previous = tx.get("leader").await.unwrap().unwrap();
    assert_eq!(previous.version, epoch);
    tx.put("leader", Bytes::from("node-b"));
    tx.put("previous-leader", previous.value);
    let handover = tx.commit().await.unwrap().epoch.unwrap();
    assert!(handover > epoch);

    // Both keys come from one commit, so they share its version.
    let mut tx = db.begin();
    let leader = tx.get("leader").await.unwrap().unwrap();
    let previous = tx.get("previous-leader").await.unwrap().unwrap();
    assert_eq!(leader.value, Bytes::from("node-b"));
    assert_eq!(previous.value, Bytes::from("node-a"));
    assert_eq!((leader.version, previous.version), (handover, handover));
    // Nothing written, nothing committed.
    assert!(tx.commit().await.unwrap().epoch.is_none());

    // Two nodes read the same key; the first commit wins, the second fails once and
    // writes nothing. Retrying is up to the caller.
    let mut a = db.begin();
    let mut b = db.begin();
    a.get("leader").await.unwrap();
    b.get("leader").await.unwrap();
    a.put("leader", Bytes::from("node-c"));
    b.put("leader", Bytes::from("node-d"));
    a.commit().await.unwrap();
    assert!(matches!(b.commit().await, Err(Error::Conflict { .. })));
    assert_eq!(
        db.get("leader").await.unwrap().unwrap().value,
        Bytes::from("node-c")
    );

    // A write to a key the transaction did not read is no conflict.
    let mut tx = db.begin();
    let leader = tx.get("leader").await.unwrap().unwrap();
    db.put("config", Bytes::from("y")).await.unwrap();
    tx.put("previous-leader", leader.value);
    tx.commit().await.unwrap();

    // Unconditional put and delete.
    db.put("config", Bytes::from("x")).await.unwrap();
    db.delete("config").await.unwrap();
    assert!(db.get("config").await.unwrap().is_none());
}
