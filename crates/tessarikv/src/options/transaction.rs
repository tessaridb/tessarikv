//! Options for one transaction.

/// Which anomalies a transaction is protected from (see the `transaction` module).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Isolation {
    /// Reads at one snapshot; the commit is refused when a key the transaction writes was
    /// written after it began (first committer wins). Admits write skew.
    #[default]
    Snapshot,
    /// As [`Isolation::Snapshot`], and the commit is also refused when a key the transaction
    /// read, or a key in a range it scanned, was written after it began: committed serializable
    /// transactions behave as if they ran one at a time, in commit order. Needs
    /// [`crate::DbOptions::serializable_transactions`].
    Serializable,
}

/// How one transaction behaves.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransactionOptions {
    /// The isolation the transaction runs at (default [`Isolation::Snapshot`]).
    pub isolation: Isolation,
}
