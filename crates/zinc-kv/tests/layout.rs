use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
use object_store::{path::Path, ObjectStore};
use zinc_fs::CasLocalFileSystem;
use zinc_kv::Db;

/// Checks the shape of what a `Db` leaves in the store: one manifest plus one immutable
/// segment per commit. The files stay under `target/tmp/layout` for inspection.
#[tokio::test]
async fn one_manifest_and_one_segment_per_commit() {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("layout");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let store = Arc::new(CasLocalFileSystem::new_with_prefix(&root).unwrap());
    let db = Db::open(store.clone(), Path::from("mesh")).await.unwrap();

    db.put("a", Bytes::from("1")).await.unwrap();
    db.put("b", Bytes::from("2")).await.unwrap();
    db.delete("a").await.unwrap();

    let top = store
        .list_with_delimiter(Some(&Path::from("mesh")))
        .await
        .unwrap();
    let names: Vec<String> = top
        .objects
        .iter()
        .map(|o| o.location.filename().unwrap().to_owned())
        .collect();
    assert!(names.contains(&"manifest.json".to_owned()), "{names:?}");
    assert!(!names.contains(&"state.json".to_owned()), "{names:?}");

    let segments = store
        .list_with_delimiter(Some(&Path::from("mesh/segments")))
        .await
        .unwrap();
    assert_eq!(segments.objects.len(), 3);
}
