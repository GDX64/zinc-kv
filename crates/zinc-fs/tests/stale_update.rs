use bytes::Bytes;
use object_store::{path::Path, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use zinc_fs::CasLocalFileSystem;

fn update_to(e_tag: Option<String>) -> PutOptions {
    PutMode::Update(UpdateVersion { e_tag, version: None }).into()
}

#[tokio::test]
async fn update_only_succeeds_with_current_etag() {
    let dir = tempfile_dir("stale_update");
    let store = CasLocalFileSystem::new_with_prefix(&dir).unwrap();
    let path = Path::from("obj");

    let v0 = store.put(&path, Bytes::from("a").into()).await.unwrap();

    // Current tag: succeeds and yields a new tag.
    let v1 = store
        .put_opts(&path, Bytes::from("b").into(), update_to(v0.e_tag.clone()))
        .await
        .unwrap();
    assert_ne!(v0.e_tag, v1.e_tag);

    // Stale tag: rejected, and the object is untouched.
    let err = store
        .put_opts(&path, Bytes::from("c").into(), update_to(v0.e_tag))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Precondition { .. }), "{err:?}");
    let body = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(body, Bytes::from("b"));
}

fn tempfile_dir(name: &str) -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
