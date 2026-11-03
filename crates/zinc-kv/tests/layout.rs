use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use zinc_kv::Db;

/// Checks the shape of what a `Db` leaves in the store: one numbered, immutable commit file
/// per write and nothing else. The files stay under `target/tmp/layout` for inspection.
#[tokio::test]
async fn one_numbered_commit_file_per_write() {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("layout");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(&root).unwrap());
    let db = Db::open(store.clone(), Path::from("mesh")).await.unwrap();

    db.put("a", Bytes::from("1")).await.unwrap();
    db.put("b", Bytes::from("2")).await.unwrap();
    db.delete("a").await.unwrap();

    let top = store
        .list_with_delimiter(Some(&Path::from("mesh")))
        .await
        .unwrap();
    assert!(top.objects.is_empty(), "{:?}", top.objects);
    assert_eq!(top.common_prefixes, vec![Path::from("mesh/log")]);

    let log = store
        .list_with_delimiter(Some(&Path::from("mesh/log")))
        .await
        .unwrap();
    let mut names: Vec<String> = log
        .objects
        .iter()
        .map(|o| o.location.filename().unwrap().to_owned())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "00000000000000000001.json",
            "00000000000000000002.json",
            "00000000000000000003.json",
        ]
    );
}
