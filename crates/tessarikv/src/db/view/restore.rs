//! The table and blob files a database holds when it opens: those its version names, and none
//! a crash left behind.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tessarikv_constants::LEVELS;

use super::TableFile;
use crate::Result;
use crate::db::block_cache::BlockCache;
use crate::db::flush_job::table_path;
use crate::db::table_blocks::open_table;
use crate::db::tables::blobs::resolve;
use crate::storage::Storage;
use crate::version::Version;

/// Opens every table file `version` names, newest first, reading their data blocks through
/// `cache`, with the blob files they hold, and removes the table files it does not name and the
/// blob files no named table holds: those a crash left between writing a table and naming it in
/// the manifest; with `remove_orphans` off those are left in place. Runs on a blocking thread.
pub(in crate::db) fn restore_tables(
    storage: &dyn Storage,
    dir: &Path,
    version: &Version,
    cache: &Arc<BlockCache>,
    remove_orphans: bool,
) -> Result<Vec<TableFile>> {
    let mut files = Vec::new();
    let mut blobs = HashMap::new();
    let keyspaces = std::iter::once(0).chain(version.keyspaces().map(|(id, _)| id));
    for keyspace in keyspaces {
        for meta in (0..LEVELS).flat_map(|level| version.tables(keyspace, level)) {
            let path = table_path(dir, meta.id);
            let (table, blocks) = open_table(storage, &path, meta.id, cache, None)?;
            let held = resolve(table.blob_files(), &mut blobs, storage, dir, cache)?;
            files.push(TableFile::new(Arc::clone(meta), table, blocks)?.with_blobs(held)?);
        }
    }
    if !remove_orphans {
        return Ok(files);
    }
    for name in storage.list(dir)? {
        let id = |suffix| {
            name.strip_suffix(suffix)
                .and_then(|stem: &str| stem.parse::<u64>().ok())
        };
        let orphan = match (id(".sst"), id(".blob")) {
            (Some(table), _) => !files.iter().any(|file| file.meta.id == table),
            (None, Some(blob)) => !blobs.contains_key(&blob),
            (None, None) => false,
        };
        if orphan {
            storage.remove(&dir.join(&name))?;
        }
    }
    Ok(files)
}
