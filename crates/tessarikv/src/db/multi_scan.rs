//! Many range scans at one snapshot.

use tessarikv_types::{KeyspaceId, SequenceNumber};

use super::Db;
use super::read::{ScanPage, ScanRange, scan_in};
use super::view::View;
use crate::{ReadOptions, Result};

impl Db {
    /// One page for each of `ranges` in `keyspace`, in the order given, all read at ONE
    /// snapshot: no batch is seen in some pages and not others, which separate [`Db::scan`]
    /// calls do not promise. Each page is limited and continued on its own, as [`Db::scan`]
    /// returns it; ranges may be empty, overlap, repeat or come in any order.
    ///
    /// # Errors
    ///
    /// As [`Db::scan`].
    pub async fn multi_scan<'a>(
        &self,
        keyspace: KeyspaceId,
        ranges: impl IntoIterator<Item = ScanRange<'a>>,
        options: &ReadOptions,
    ) -> Result<Vec<ScanPage>> {
        self.check_keyspace(keyspace)?;
        // The view before the sequence, as in `Db::get`: one of each for every range.
        let view = self.view.load();
        let snapshot = self.visible_sequence()?;
        scan_all(&view, keyspace, ranges, snapshot, options).await
    }

    /// One page for each of `ranges` at `snapshot`, as [`Db::multi_scan`] returns them.
    pub(super) async fn multi_scan_at<'a>(
        &self,
        keyspace: KeyspaceId,
        ranges: impl IntoIterator<Item = ScanRange<'a>>,
        snapshot: SequenceNumber,
        options: &ReadOptions,
    ) -> Result<Vec<ScanPage>> {
        self.check_keyspace(keyspace)?;
        let view = self.view.load();
        scan_all(&view, keyspace, ranges, snapshot, options).await
    }
}

/// Reads a page of every range of `ranges` from `view` at `snapshot`, in order.
async fn scan_all<'a>(
    view: &View,
    keyspace: KeyspaceId,
    ranges: impl IntoIterator<Item = ScanRange<'a>>,
    snapshot: SequenceNumber,
    options: &ReadOptions,
) -> Result<Vec<ScanPage>> {
    let ranges = ranges.into_iter();
    let mut pages = Vec::with_capacity(ranges.size_hint().0);
    for range in ranges {
        pages.push(scan_in(view, keyspace, range, snapshot, options.fill_cache).await?);
    }
    Ok(pages)
}
