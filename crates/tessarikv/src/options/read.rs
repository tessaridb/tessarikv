//! Options for one read.

/// How one read or iterator behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReadOptions {
    /// Has no effect: every block read from a table is verified against its checksum whatever
    /// this says, and a corrupt one is a [`crate::ErrorCategory::Corruption`] error. Kept so code
    /// that sets it still builds (default `true`).
    pub verify_checksums: bool,
    /// Keep the blocks this read loads in the block cache (default `true`).
    ///
    /// A one-off scan sets it to `false` so it does not evict the working set.
    pub fill_cache: bool,
    /// The least key an iterator shows (inclusive); `None` for no bound. Ignored by point reads
    /// and scans, which take their range as arguments.
    pub lower_bound: Option<Vec<u8>>,
    /// The key an iterator stops before (exclusive); `None` for no bound. Ignored by point
    /// reads and scans.
    pub upper_bound: Option<Vec<u8>>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            verify_checksums: true,
            fill_cache: true,
            lower_bound: None,
            upper_bound: None,
        }
    }
}

impl ReadOptions {
    /// Bounds an iterator to the keys that start with `prefix`.
    ///
    /// ```
    /// use tessarikv::ReadOptions;
    ///
    /// let options = ReadOptions::default().with_prefix(b"user/");
    /// assert_eq!(options.lower_bound.as_deref(), Some(&b"user/"[..]));
    /// assert_eq!(options.upper_bound.as_deref(), Some(&b"user0"[..]));
    /// ```
    #[must_use]
    pub fn with_prefix(mut self, prefix: &[u8]) -> Self {
        self.lower_bound = Some(prefix.to_vec());
        self.upper_bound = prefix_end(prefix);
        self
    }
}

/// The least key greater than every key that starts with `prefix`: the prefix with its last
/// byte below `0xff` incremented and the rest dropped. `None` when no such key exists (the
/// prefix is empty or all `0xff`), so the range has no upper end.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|&byte| byte != u8::MAX)?;
    let mut end = prefix.get(..=last)?.to_vec();
    let byte = end.last_mut()?;
    *byte = byte.checked_add(1)?;
    Some(end)
}

#[cfg(test)]
mod tests {
    use super::prefix_end;

    #[test]
    fn a_prefix_ends_at_its_least_successor() {
        assert_eq!(prefix_end(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(b"a\xff\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_end(b"\xff\xff"), None);
        assert_eq!(prefix_end(b""), None);
    }
}
