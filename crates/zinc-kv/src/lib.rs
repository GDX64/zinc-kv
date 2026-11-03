//! A small strongly consistent key-value store on top of `object_store`.
//!
//! Layout under the prefix:
//! - `manifest.json`: the only mutable object, replaced with a conditional put.
//! - `segments/<id>.json`: immutable batches of writes, one per commit.
//!
//! A commit uploads a segment, then wins by appending it to the manifest.

mod error;
mod manifest;
mod segment;
mod types;

use std::sync::Arc;

use bytes::Bytes;
use object_store::{
    path::Path, Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
};

pub use error::{CasError, Error, Result};
pub use types::{Entry, Version};

use manifest::{Manifest, SegmentRef};
use segment::{Op, Segment};

const MAX_ATTEMPTS: usize = 64;

pub struct Db {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

/// Manifest as read from the store, plus the tag needed to conditionally replace it.
struct Snapshot {
    manifest: Manifest,
    /// `None` if the manifest does not exist yet.
    tag: Option<UpdateVersion>,
}

/// What the newest segment mentioning a key says about it.
struct Found {
    version: Version,
    /// `None` for a tombstone.
    value: Option<Bytes>,
}

impl Found {
    fn live_version(&self) -> Option<Version> {
        self.value.as_ref().map(|_| self.version)
    }
}

impl Db {
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> Result<Db> {
        Ok(Db { store, prefix })
    }

    pub async fn get(&self, key: &str) -> Result<Option<Entry>> {
        let snap = self.load().await?;
        let found = self.find(&snap.manifest.segments, key).await?;
        Ok(found.and_then(|f| {
            let version = f.version;
            f.value.map(|value| Entry { value, version })
        }))
    }

    /// Unconditional write. Returns the new version.
    pub async fn put(&self, key: &str, value: Bytes) -> Result<Version> {
        self.commit(key, None, Some(value))
            .await
            .map_err(unwrap_other)
    }

    /// Unconditional delete.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.commit(key, None, None).await.map_err(unwrap_other)?;
        Ok(())
    }

    /// Compare-and-set. `expected: None` means the key must not exist.
    /// `new: None` deletes the key. Returns the new version.
    pub async fn cas(
        &self,
        key: &str,
        expected: Option<Version>,
        new: Option<Bytes>,
    ) -> std::result::Result<Version, CasError> {
        self.commit(key, Some(expected), new).await
    }

    /// `expected: None` skips the precondition check.
    async fn commit(
        &self,
        key: &str,
        expected: Option<Option<Version>>,
        new: Option<Bytes>,
    ) -> std::result::Result<Version, CasError> {
        let mut snap = self.load().await?;

        // Version of the key as of `snap`; only tracked when there is a precondition.
        let mut current = None;
        if let Some(expected) = expected {
            current = self
                .find(&snap.manifest.segments, key)
                .await?
                .and_then(|f| f.live_version());
            if expected != current {
                return Err(CasError::Conflict { current });
            }
        }

        let segment_path = self.upload_segment(key, new).await?;

        for _ in 0..MAX_ATTEMPTS {
            let commit_epoch = snap.manifest.epoch + 1;
            let mut next = snap.manifest.clone();
            next.epoch = commit_epoch;
            next.segments.push(SegmentRef {
                path: segment_path.to_string(),
                commit_epoch,
            });

            if self.store_manifest(&next, snap.tag.clone()).await? {
                return Ok(Version::from_epoch(commit_epoch));
            }

            // Lost the race: reload, and re-check the precondition against only the
            // segments that were added since our snapshot.
            let seen = snap.manifest.segments.len();
            snap = self.load().await?;
            if let Some(expected) = expected {
                if let Some(found) = self.find(&snap.manifest.segments[seen..], key).await? {
                    current = found.live_version();
                }
                if expected != current {
                    return Err(CasError::Conflict { current });
                }
            }
        }
        Err(Error::Contention.into())
    }

    /// Newest segment entry for `key` among `segments` (ordered oldest to newest).
    async fn find(&self, segments: &[SegmentRef], key: &str) -> Result<Option<Found>> {
        for seg_ref in segments.iter().rev() {
            let bytes = self
                .store
                .get(&Path::from(seg_ref.path.as_str()))
                .await?
                .bytes()
                .await?;
            let segment: Segment = serde_json::from_slice(&bytes)?;
            // A segment may mention a key more than once; the last op wins.
            if let Some(op) = segment.ops.into_iter().rev().find(|op| op.key() == key) {
                return Ok(Some(Found {
                    version: Version::from_epoch(seg_ref.commit_epoch),
                    value: match op {
                        Op::Put { value, .. } => Some(value),
                        Op::Delete { .. } => None,
                    },
                }));
            }
        }
        Ok(None)
    }

    async fn upload_segment(&self, key: &str, new: Option<Bytes>) -> Result<Path> {
        let op = match new {
            Some(value) => Op::Put {
                key: key.to_owned(),
                value,
            },
            None => Op::Delete {
                key: key.to_owned(),
            },
        };
        let path = self
            .prefix
            .clone()
            .join("segments")
            .join(format!("{}.json", uuid::Uuid::now_v7()));
        let payload = Bytes::from(serde_json::to_vec(&Segment { ops: vec![op] })?);
        self.store
            .put_opts(&path, payload.into(), PutMode::Create.into())
            .await?;
        Ok(path)
    }

    fn manifest_path(&self) -> Path {
        self.prefix.clone().join("manifest.json")
    }

    async fn load(&self) -> Result<Snapshot> {
        match self.store.get(&self.manifest_path()).await {
            Ok(res) => {
                let tag = UpdateVersion {
                    e_tag: res.meta.e_tag.clone(),
                    version: res.meta.version.clone(),
                };
                let bytes = res.bytes().await?;
                Ok(Snapshot {
                    manifest: serde_json::from_slice(&bytes)?,
                    tag: Some(tag),
                })
            }
            Err(StoreError::NotFound { .. }) => Ok(Snapshot {
                manifest: Manifest::default(),
                tag: None,
            }),
            Err(e) => Err(e.into()),
        }
    }

    /// Returns `false` if someone else replaced the manifest since we loaded it.
    async fn store_manifest(
        &self,
        manifest: &Manifest,
        tag: Option<UpdateVersion>,
    ) -> Result<bool> {
        let mode = match tag {
            None => PutMode::Create,
            Some(v) => PutMode::Update(v),
        };
        let payload = Bytes::from(serde_json::to_vec(manifest)?);
        match self
            .store
            .put_opts(&self.manifest_path(), payload.into(), mode.into())
            .await
        {
            Ok(_) => Ok(true),
            Err(StoreError::Precondition { .. } | StoreError::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// Unconditional ops never produce `Conflict`.
fn unwrap_other(e: CasError) -> Error {
    match e {
        CasError::Other(e) => e,
        CasError::Conflict { .. } => unreachable!("unconditional commit cannot conflict"),
    }
}
