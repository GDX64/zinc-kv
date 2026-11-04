//! An `ObjectStore` wrapper that counts requests by kind, and a cost model that turns the
//! counts into an estimated bill and latency for a real object store.

use std::{
    fmt,
    ops::{Add, Sub},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};

/// Requests made, by kind. A listing counts once, however many pages a real store would
/// return.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Calls {
    pub get: u64,
    pub head: u64,
    pub put: u64,
    pub list: u64,
    pub delete: u64,
}

impl Calls {
    pub fn total(&self) -> u64 {
        self.get + self.head + self.put + self.list + self.delete
    }

    /// Per operation, over `ops` operations.
    pub fn per_op(&self, ops: u64) -> [f64; 5] {
        let ops = ops as f64;
        [self.get, self.head, self.put, self.list, self.delete].map(|n| n as f64 / ops)
    }
}

impl Add for Calls {
    type Output = Calls;

    fn add(self, other: Calls) -> Calls {
        Calls {
            get: self.get + other.get,
            head: self.head + other.head,
            put: self.put + other.put,
            list: self.list + other.list,
            delete: self.delete + other.delete,
        }
    }
}

impl Sub for Calls {
    type Output = Calls;

    fn sub(self, before: Calls) -> Calls {
        Calls {
            get: self.get - before.get,
            head: self.head - before.head,
            put: self.put - before.put,
            list: self.list - before.list,
            delete: self.delete - before.delete,
        }
    }
}

/// What each kind of request costs: money per million requests, and an assumed round
/// trip. A first model: S3 Standard request prices, rough same-region latencies, and every
/// request in sequence (true of the crate today), so latency adds up.
pub struct CostModel {
    pub name: &'static str,
    /// Dollars per million requests: get, head, put, list, delete.
    pub dollars_per_million: [f64; 5],
    /// Milliseconds per request: get, head, put, list, delete.
    pub millis: [f64; 5],
}

pub const S3: CostModel = CostModel {
    name: "S3 Standard",
    // GET/HEAD $0.0004 per 1,000; PUT/LIST $0.005 per 1,000; DELETE free.
    dollars_per_million: [0.4, 0.4, 5.0, 5.0, 0.0],
    millis: [20.0, 15.0, 40.0, 40.0, 20.0],
};

impl CostModel {
    /// Dollars per million operations, given the calls per operation.
    pub fn dollars(&self, per_op: [f64; 5]) -> f64 {
        per_op
            .iter()
            .zip(self.dollars_per_million)
            .map(|(n, d)| n * d)
            .sum()
    }

    /// Estimated milliseconds per operation, given the calls per operation.
    pub fn millis(&self, per_op: [f64; 5]) -> f64 {
        per_op.iter().zip(self.millis).map(|(n, ms)| n * ms).sum()
    }
}

/// Waits `delay` precisely. A timer sleep would round up to the OS timer resolution (about
/// 15 ms on Windows), so this yields to other tasks until the deadline instead, at the cost
/// of CPU while waiting.
pub async fn wait(delay: Duration) {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

/// Wraps a store and counts every request that reaches it, optionally holding each one for
/// a fixed delay first, like a remote store's round trip.
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    calls: Arc<Mutex<Calls>>,
    delay: Duration,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Self::with_delay(inner, Duration::ZERO)
    }

    pub fn with_delay(inner: Arc<dyn ObjectStore>, delay: Duration) -> Arc<Self> {
        Arc::new(CountingStore {
            inner,
            calls: Arc::default(),
            delay,
        })
    }

    pub fn calls(&self) -> Calls {
        *self.calls.lock().unwrap()
    }

    fn count(&self, f: impl FnOnce(&mut Calls)) {
        f(&mut self.calls.lock().unwrap());
    }

    /// A listing that starts only after the delay.
    fn delayed(
        &self,
        listing: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let delay = self.delay;
        stream::once(async move {
            wait(delay).await;
            listing
        })
        .flatten()
        .boxed()
    }
}

impl fmt::Debug for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.count(|c| c.put += 1);
        wait(self.delay).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.count(|c| c.put += 1);
        wait(self.delay).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.count(|c| {
            if options.head {
                c.head += 1
            } else {
                c.get += 1
            }
        });
        wait(self.delay).await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let calls = self.calls.clone();
        let delay = self.delay;
        let counted = locations
            .inspect(move |_| calls.lock().unwrap().delete += 1)
            .then(move |location| async move {
                wait(delay).await;
                location
            })
            .boxed();
        self.inner.delete_stream(counted)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.count(|c| c.list += 1);
        self.delayed(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.count(|c| c.list += 1);
        self.delayed(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.count(|c| c.list += 1);
        wait(self.delay).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.count(|c| c.put += 1);
        wait(self.delay).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.count(|c| c.put += 1);
        wait(self.delay).await;
        self.inner.rename_opts(from, to, options).await
    }
}
