use std::sync::Arc;

use bytes::Bytes;
use object_store::{memory::InMemory, path::Path};
use zinc_kv::Db;

#[tokio::test]
async fn put_get_cas_delete() {
    let store = Arc::new(InMemory::new());
    let db = Db::open(store, Path::from("mesh")).await.unwrap();

    // Missing key.
    assert!(db.get("leader").await.unwrap().is_none());

    // Create via CAS: `None` means "must not exist".
    let v1 = db
        .cas("leader", None, Some(Bytes::from("node-a")))
        .await
        .unwrap();

    let entry = db.get("leader").await.unwrap().unwrap();
    assert_eq!(entry.value, Bytes::from("node-a"));
    assert_eq!(entry.version, v1);

    // Update with the version we saw.
    let v2 = db
        .cas("leader", Some(v1), Some(Bytes::from("node-b")))
        .await
        .unwrap();
    assert!(v2 > v1);

    // Unconditional put and delete.
    db.put("config", Bytes::from("x")).await.unwrap();
    db.delete("config").await.unwrap();
    assert!(db.get("config").await.unwrap().is_none());
}
