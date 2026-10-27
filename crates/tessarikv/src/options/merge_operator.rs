//! A keyspace's merge operator: what a key holds after operands were added to it.

use std::fmt;
use std::sync::Arc;

use super::keyspace::KeyspaceOptions;
use super::keyspace_builder::KeyspaceOptionsBuilder;

/// Combines the operands written with [`crate::WriteBatch::merge`] with the value below them.
///
/// A read of a key whose newest versions are operands asks the operator for its value: `base`
/// is the value of the newest version below the operands that is not one — `None` when that
/// version is a delete, an expired put, or there is none — and `operands` are the operands
/// above it, oldest first. The value read is what the operator returns. Compaction may ask the
/// same question and write the answer as a put in place of the operands.
///
/// The answer must depend only on the key, the base and the operands, and be the same each
/// time: a read may ask again, and compaction may ask for any run of operands that has its
/// base. The operator runs on reading tasks and on compaction threads, possibly several at
/// once, so it is `Send + Sync`, and it must not call the database. The meaning of an operand
/// is fixed once stored: an open of the keyspace must name an operator that reads its operands.
pub trait MergeOperator: Send + Sync {
    /// A name for the options record.
    fn name(&self) -> &str;

    /// The value `key` holds after `operands`, oldest first, were added to `base`; `None` when
    /// the operator cannot combine them, which a read reports as [`crate::Error::MergeFailed`].
    fn full_merge(&self, key: &[u8], base: Option<&[u8]>, operands: &[&[u8]]) -> Option<Vec<u8>>;
}

/// A shared operator in a keyspace's options: equal when it is the same operator, shown by
/// name.
#[derive(Clone)]
pub(crate) struct OperatorHandle(pub(crate) Arc<dyn MergeOperator>);

impl PartialEq for OperatorHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for OperatorHandle {}

impl fmt::Debug for OperatorHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.name())
    }
}

impl KeyspaceOptions {
    /// The keyspace's merge operator; default `None`, and a merge written to the keyspace is
    /// then refused.
    #[must_use]
    pub fn merge_operator(&self) -> Option<&Arc<dyn MergeOperator>> {
        self.merge_operator.as_ref().map(|handle| &handle.0)
    }
}

impl KeyspaceOptionsBuilder {
    /// Sets [`KeyspaceOptions::merge_operator`].
    pub fn merge_operator(mut self, operator: Arc<dyn MergeOperator>) -> Self {
        self.options.merge_operator = Some(OperatorHandle(operator));
        self
    }
}
