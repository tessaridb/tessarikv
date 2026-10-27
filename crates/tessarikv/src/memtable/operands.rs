//! A table's side of merge operands: the keyspace's operator, carried by each of its tables so a
//! read and a write find it in the view, and the versions of one key a read combines.

use std::sync::Arc;

use tessarikv_types::{InternalKeyRef, SequenceNumber, ValueKind, encode_internal, seek_trailer};

use tessarikv_sync::Cursor;

use super::{InternalKeyComparator, Memtable};
use crate::MergeOperator;

impl Memtable {
    /// This table with `operator` as its keyspace's merge operator.
    #[must_use]
    pub(crate) fn with_operator(mut self, operator: Option<Arc<dyn MergeOperator>>) -> Self {
        self.operator = operator;
        self
    }

    /// Its keyspace's merge operator, if it has one.
    pub(crate) fn operator(&self) -> Option<&Arc<dyn MergeOperator>> {
        self.operator.as_ref()
    }

    /// The versions of `key` at or below `snapshot`, newest first.
    pub(crate) fn versions(&self, key: &[u8], snapshot: SequenceNumber) -> KeyVersions<'_> {
        let mut cursor = self.entries.cursor();
        let mut target = Vec::new();
        encode_internal(key, seek_trailer(snapshot), &mut target);
        cursor.seek(&target);
        KeyVersions {
            cursor,
            key: key.to_vec(),
        }
    }
}

/// The versions of one key in a table, newest first (see [`Memtable::versions`]).
pub(crate) struct KeyVersions<'a> {
    cursor: Cursor<'a, InternalKeyComparator>,
    key: Vec<u8>,
}

impl<'a> Iterator for KeyVersions<'a> {
    /// Sequence, kind and stored value.
    type Item = (SequenceNumber, ValueKind, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor.valid() {
            let parsed = InternalKeyRef::parse(self.cursor.key()).ok()?;
            if parsed.user_key() != self.key.as_slice() {
                return None;
            }
            let version = parsed
                .trailer()
                .kind()
                .ok()
                .map(|kind| (parsed.trailer().sequence(), kind, self.cursor.value()));
            self.cursor.next();
            if version.is_some() {
                return version;
            }
        }
        None
    }
}
