//! Range reads inside a transaction: pages of the snapshot merged with the transaction's own
//! writes.

use std::cmp::Ordering;

use tessarikv_types::KeyspaceId;

use super::Transaction;
use super::write_set::Own;
use crate::{Db, ReadOptions, Result, ScanPage};

impl Transaction<'_> {
    /// Up to `limit` live entries of `keyspace` with keys in `[start, end)` (unbounded above
    /// when `end` is `None`) as this transaction sees them: the database as it was when the
    /// transaction began, with the transaction's own puts, deletes and range deletes applied. A
    /// full page names the key the next page starts at, as [`Db::scan`] does.
    ///
    /// # Errors
    ///
    /// As [`Db::scan`].
    pub async fn scan(
        &mut self,
        keyspace: KeyspaceId,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        options: &ReadOptions,
    ) -> Result<ScanPage> {
        Db::check_keyspace(keyspace)?;
        let page = self
            .merged_page(keyspace, start, end, limit, options)
            .await?;
        if let Some(reads) = &mut self.reads {
            // The range this page answered for: up to where the next page starts.
            reads.range(keyspace, start, page.next.as_deref().or(end));
        }
        Ok(page)
    }

    /// The page [`Transaction::scan`] returns.
    async fn merged_page(
        &self,
        keyspace: KeyspaceId,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        options: &ReadOptions,
    ) -> Result<ScanPage> {
        let mut page = ScanPage::default();
        let mut cursor = start.to_vec();
        loop {
            // Each snapshot page covers `[cursor, its next)`, or up to `end` when it is the last.
            let snapshot = self
                .snapshot
                .scan(keyspace, &cursor, end, limit.max(1), options)
                .await?;
            let covered = snapshot.next.as_deref().or(end);
            let own = self.writes.values_in(keyspace, &cursor, covered);
            let mut theirs = snapshot.entries.into_iter().peekable();
            let mut ours = own.into_iter().peekable();
            loop {
                let candidate = match (theirs.peek(), ours.peek()) {
                    (None, None) => break,
                    (Some((key, _)), Some((own_key, _))) => key.as_slice().cmp(own_key),
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                };
                let (key, value) = match candidate {
                    Ordering::Less => match theirs.next() {
                        Some((key, value)) => match self.writes.lookup(keyspace, &key) {
                            Own::Untouched => (key, value),
                            // A key this transaction wrote is among its own values, or deleted.
                            Own::Value(_) | Own::Deleted => continue,
                        },
                        None => break,
                    },
                    Ordering::Equal => {
                        theirs.next();
                        match ours.next() {
                            Some((key, value)) => (key.to_vec(), value.to_vec()),
                            None => break,
                        }
                    }
                    Ordering::Greater => match ours.next() {
                        Some((key, value)) => (key.to_vec(), value.to_vec()),
                        None => break,
                    },
                };
                if page.entries.len() >= limit {
                    page.next = Some(key);
                    return Ok(page);
                }
                page.entries.push((key, value));
            }
            match snapshot.next {
                Some(next) => cursor = next,
                None => return Ok(page),
            }
        }
    }
}
