//! One input's walk through a compaction: its blocks read on the job's thread.

use tessarikv_format::TableIter;

use crate::db::tables::format_error;
use crate::db::view::TableFile;
use crate::{Error, Result};

/// One input's walk.
pub(super) struct Walk<'a> {
    pub(super) versions: TableIter,
    pub(super) file: &'a TableFile,
}

/// Loads the blocks the last move of `versions` waits for, and reports a damaged block.
pub(super) fn settle(file: &TableFile, versions: &mut TableIter) -> Result<()> {
    file.blocks.complete_blocking(versions)?;
    versions
        .status()
        .map_err(|error| format_error(&error, None))
}

pub(super) fn corrupt() -> Error {
    Error::Corruption {
        what: "table",
        path: None,
        offset: None,
        detail: "a stored key has no trailer or a kind this build does not read".to_owned(),
    }
}
