use serde::{Deserialize, Serialize};

/// The only mutable object. Segments are listed oldest to newest.
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub epoch: u64,
    pub segments: Vec<SegmentRef>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SegmentRef {
    pub path: String,
    /// Epoch of the manifest write that added this segment; the version of every key it wrote.
    pub commit_epoch: u64,
}
