//! The key a point read looks up in table files.

use tessarikv_constants::TRAILER_BYTES;
use tessarikv_types::{SequenceNumber, encode_internal, seek_trailer};

/// A key and the internal key a read seeks in every table file at its snapshot, encoded once per
/// read rather than once per file.
#[derive(Debug)]
pub(crate) struct LookupKey {
    target: Vec<u8>,
    key_len: usize,
}

impl LookupKey {
    pub(crate) fn new(key: &[u8], snapshot: SequenceNumber) -> Self {
        let mut target = Vec::with_capacity(key.len().saturating_add(TRAILER_BYTES));
        encode_internal(key, seek_trailer(snapshot), &mut target);
        Self {
            target,
            key_len: key.len(),
        }
    }

    pub(super) fn target(&self) -> &[u8] {
        &self.target
    }

    pub(super) fn user_key(&self) -> &[u8] {
        self.target.get(..self.key_len).unwrap_or_default()
    }
}
