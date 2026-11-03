use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{de::Error as _, Deserialize, Serialize};

use crate::{
    commit::{b64, Op},
    error::{Error, Result},
    Entry, Version,
};

/// Format version written into every checkpoint file.
pub(crate) const FORMAT: u64 = 1;

/// The live state as of commit `number`: every key with its value and the version that
/// wrote it. Tombstones are dropped. A pure function of commits `1..=number`, so every
/// writer of a given checkpoint produces the same bytes.
#[derive(Clone, Default)]
pub(crate) struct Checkpoint {
    pub number: u64,
    pub entries: BTreeMap<String, Entry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    v: u64,
    /// Sorted by key.
    entries: Vec<FileEntry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileEntry {
    key: String,
    #[serde(with = "b64")]
    value: Bytes,
    version: Version,
}

impl Checkpoint {
    /// Applies commit `number`, which must be the one right after this state.
    pub fn apply(&mut self, number: u64, ops: &[Op]) {
        debug_assert_eq!(number, self.number + 1);
        for op in ops {
            match op {
                Op::Put { key, value } => {
                    let entry = Entry {
                        value: value.clone(),
                        version: Version::from_commit(number),
                    };
                    self.entries.insert(key.clone(), entry);
                }
                Op::Delete { key } => {
                    self.entries.remove(key);
                }
            }
        }
        self.number = number;
    }

    pub fn encode(&self) -> Result<Bytes> {
        let file = File {
            v: FORMAT,
            entries: self
                .entries
                .iter()
                .map(|(key, entry)| FileEntry {
                    key: key.clone(),
                    value: entry.value.clone(),
                    version: entry.version,
                })
                .collect(),
        };
        Ok(Bytes::from(serde_json::to_vec(&file)?))
    }

    /// Parses checkpoint `number`, checking the format version first (as for commits),
    /// then that the entries are sorted, unique, and written no later than `number`.
    pub fn decode(number: u64, bytes: &[u8]) -> Result<Checkpoint> {
        #[derive(Deserialize)]
        struct Header {
            v: u64,
        }
        let corrupt = |source| Error::CorruptCheckpoint { number, source };
        let header: Header = serde_json::from_slice(bytes).map_err(corrupt)?;
        if header.v != FORMAT {
            return Err(Error::UnsupportedCheckpointFormat {
                number,
                version: header.v,
            });
        }
        let file: File = serde_json::from_slice(bytes).map_err(corrupt)?;
        let mut entries = BTreeMap::new();
        for entry in file.entries {
            if entry.version > Version::from_commit(number) {
                return Err(corrupt(serde_json::Error::custom(format!(
                    "key {:?} has version {:?}, newer than the checkpoint",
                    entry.key, entry.version
                ))));
            }
            if entries
                .last_key_value()
                .is_some_and(|(last, _)| *last >= entry.key)
            {
                return Err(corrupt(serde_json::Error::custom(format!(
                    "key {:?} is out of order or repeated",
                    entry.key
                ))));
            }
            entries.insert(
                entry.key,
                Entry {
                    value: entry.value,
                    version: entry.version,
                },
            );
        }
        Ok(Checkpoint { number, entries })
    }
}
