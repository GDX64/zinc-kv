//! **Test-only.** A local-filesystem `ObjectStore` that supports `PutMode::Update`.
//!
//! `object_store::local::LocalFileSystem` returns `NotImplemented` for conditional
//! updates. This wrapper adds them by serializing `Update` puts behind a mutex:
//! compare the current e_tag with the expected one, then do the normal atomic write.
//!
//! Limits, acceptable because this is only used in tests:
//! - the lock is in-process, so all users of a directory must share one `CasLocalFileSystem`
//!   (clone the `Arc`), not merely point at the same path;
//! - e_tags are `LocalFileSystem`'s (inode, mtime, size), not content hashes.

use std::fmt;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path, CopyOptions, Error, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct CasLocalFileSystem {
    inner: LocalFileSystem,
    update_lock: Mutex<()>,
}

impl CasLocalFileSystem {
    pub fn new_with_prefix(prefix: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self {
            inner: LocalFileSystem::new_with_prefix(prefix)?,
            update_lock: Mutex::new(()),
        })
    }
}

impl fmt::Display for CasLocalFileSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CasLocalFileSystem({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CasLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutMode::Update(expected) = &opts.mode else {
            return self.inner.put_opts(location, payload, opts).await;
        };

        let _guard = self.update_lock.lock().await;
        let current = match self.inner.head(location).await {
            Ok(meta) => meta,
            Err(Error::NotFound { .. }) => {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: "object does not exist".into(),
                })
            }
            Err(e) => return Err(e),
        };
        if expected.e_tag != current.e_tag {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: format!(
                    "e_tag mismatch: expected {:?}, found {:?}",
                    expected.e_tag, current.e_tag
                )
                .into(),
            });
        }
        let opts = PutOptions {
            mode: PutMode::Overwrite,
            ..opts
        };
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
