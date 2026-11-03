use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path};

use zinc_kv::Db;

/// Same flow as the in-memory happy path, but on the local filesystem under
/// `target/tmp/on_disk`, left in place afterwards so the files can be inspected.
#[tokio::test]
async fn put_get_cas_delete_on_disk() {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("on_disk");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(&root).unwrap());
    let db = Db::open(store, Path::from("mesh")).await.unwrap();

    assert!(db.get("leader").await.unwrap().is_none());

    let v1 = db
        .cas("leader", None, Some(Bytes::from("node-a")))
        .await
        .unwrap();
    let entry = db.get("leader").await.unwrap().unwrap();
    assert_eq!(entry.value, Bytes::from("node-a"));
    assert_eq!(entry.version, v1);

    let v2 = db
        .cas("leader", Some(v1), Some(Bytes::from("node-b")))
        .await
        .unwrap();
    assert!(v2 > v1);

    // Leave a key behind so state.json has content to look at.
    db.put("config", Bytes::from("x")).await.unwrap();
    db.put("scratch", Bytes::from("y")).await.unwrap();
    db.delete("scratch").await.unwrap();
    assert!(db.get("scratch").await.unwrap().is_none());
}
