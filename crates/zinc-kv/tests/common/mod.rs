//! Shared test support: backends and a fault-injecting `ObjectStore` wrapper.
#![allow(dead_code)]

use std::{
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, memory::InMemory, path::Path, CopyOptions, Error, GetOptions,
    GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use tokio::sync::oneshot;
use zinc_kv::Db;

pub const PREFIX: &str = "mesh";

#[derive(Clone, Copy, Debug)]
pub enum Backend {
    Memory,
    Disk,
}

pub const BACKENDS: [Backend; 2] = [Backend::Memory, Backend::Disk];

/// A fresh, empty store. On disk it lives under `target/tmp/<name>-disk`, wiped first and
/// left behind for inspection.
pub fn store(backend: Backend, name: &str) -> Arc<dyn ObjectStore> {
    match backend {
        Backend::Memory => Arc::new(InMemory::new()),
        Backend::Disk => Arc::new(disk(&format!("{name}-disk")).0),
    }
}

/// A fresh local store plus its root directory, for tests that tamper with files.
pub fn disk(name: &str) -> (LocalFileSystem, PathBuf) {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    (LocalFileSystem::new_with_prefix(&root).unwrap(), root)
}

pub async fn open(store: &Arc<dyn ObjectStore>) -> Db {
    Db::open(store.clone(), Path::from(PREFIX)).await.unwrap()
}

/// Names of the files in the log, sorted.
pub async fn log_files(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let listing = store
        .list_with_delimiter(Some(&Path::from(format!("{PREFIX}/log"))))
        .await
        .unwrap();
    let mut names: Vec<String> = listing
        .objects
        .iter()
        .map(|o| o.location.filename().unwrap().to_owned())
        .collect();
    names.sort();
    names
}

pub fn commit_name(number: u64) -> String {
    format!("{number:020}.json")
}

pub fn text(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// What the next `PutMode::Create` does instead of a plain create.
#[derive(Clone, Copy, Debug)]
pub enum CreateFault {
    /// Returns an error without writing.
    FailBeforeWrite,
    /// Writes, then returns an error (the response was lost).
    FailAfterWrite,
    /// Writes, then returns `AlreadyExists` (a client retry of a request that had landed).
    AlreadyExistsAfterWrite,
    /// Returns an error; the write lands just before the next listing (a request that
    /// was still in flight).
    FailThenLandLater,
}

/// Handle on a paused `Create`: await `reached`, then send on `resume`.
pub struct Paused {
    pub reached: oneshot::Receiver<()>,
    pub resume: oneshot::Sender<()>,
}

#[derive(Default)]
struct Faults {
    pause: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    next_create: Option<CreateFault>,
    every_create: Option<CreateFault>,
    in_flight: Option<(Path, PutPayload)>,
    steal_every_create: bool,
    fail_list: bool,
    vanished: Option<String>,
}

/// Wraps a store and injects faults into `PutMode::Create` and listing.
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    faults: Mutex<Faults>,
}

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(FaultStore {
            inner,
            faults: Mutex::new(Faults::default()),
        })
    }

    /// The next `Create` stops before reaching the store until resumed.
    pub fn pause_next_create(&self) -> Paused {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        self.faults.lock().unwrap().pause = Some((reached_tx, resume_rx));
        Paused {
            reached: reached_rx,
            resume: resume_tx,
        }
    }

    pub fn fail_next_create(&self, fault: CreateFault) {
        self.faults.lock().unwrap().next_create = Some(fault);
    }

    pub fn fail_every_create(&self, fault: Option<CreateFault>) {
        self.faults.lock().unwrap().every_create = fault;
    }

    /// Before every `Create`, a competitor creates the same path first.
    pub fn steal_every_create(&self) {
        self.faults.lock().unwrap().steal_every_create = true;
    }

    pub fn fail_list(&self, fail: bool) {
        self.faults.lock().unwrap().fail_list = fail;
    }

    /// Reads of the file named `name` report `NotFound` although listings still show it,
    /// as if it was deleted between the listing and the read.
    pub fn vanish_on_get(&self, name: &str) {
        self.faults.lock().unwrap().vanished = Some(name.to_owned());
    }
}

fn injected() -> Error {
    Error::Generic {
        store: "FaultStore",
        source: "injected fault".into(),
    }
}

/// A valid commit written by someone else.
fn competitor_commit() -> PutPayload {
    PutPayload::from_static(br#"{"v":1,"id":"competitor","ops":[{"op":"put","key":"competitor","value":""}]}"#)
}

impl fmt::Debug for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if !matches!(opts.mode, PutMode::Create) {
            return self.inner.put_opts(location, payload, opts).await;
        }

        let (pause, fault, steal) = {
            let mut faults = self.faults.lock().unwrap();
            (
                faults.pause.take(),
                faults.next_create.take().or(faults.every_create),
                faults.steal_every_create,
            )
        };
        if let Some((reached, resume)) = pause {
            let _ = reached.send(());
            let _ = resume.await;
        }
        if steal {
            let _ = self
                .inner
                .put_opts(location, competitor_commit(), PutMode::Create.into())
                .await;
        }
        match fault {
            None => self.inner.put_opts(location, payload, opts).await,
            Some(CreateFault::FailBeforeWrite) => Err(injected()),
            Some(CreateFault::FailAfterWrite) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(injected())
            }
            Some(CreateFault::AlreadyExistsAfterWrite) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(Error::AlreadyExists {
                    path: location.to_string(),
                    source: "injected fault".into(),
                })
            }
            Some(CreateFault::FailThenLandLater) => {
                self.faults.lock().unwrap().in_flight = Some((location.clone(), payload));
                Err(injected())
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let vanished = self.faults.lock().unwrap().vanished.clone();
        if vanished.is_some() && location.filename() == vanished.as_deref() {
            return Err(Error::NotFound {
                path: location.to_string(),
                source: "injected fault".into(),
            });
        }
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
        let (fail, in_flight) = {
            let mut faults = self.faults.lock().unwrap();
            (faults.fail_list, faults.in_flight.take())
        };
        if fail {
            return Err(injected());
        }
        if let Some((location, payload)) = in_flight {
            // Lands only if the name is still free, like any `Create`.
            let _ = self
                .inner
                .put_opts(&location, payload, PutMode::Create.into())
                .await;
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
