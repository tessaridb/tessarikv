//! A flush's blocking part: the table file, synced, and the manifest edit naming it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_format::{Compression, ManifestEdit, TableMeta, TableOptions};

use super::tables::{build, open};
use super::view::TableFile;
use crate::memtable::Memtable;
use crate::storage::Storage;
use crate::version::manifest::ManifestStore;
use crate::{Error, KeyspaceOptions, Result};

/// A flush's blocking part: the table file and the manifest edit.
pub(super) struct Job {
    pub(super) storage: Arc<dyn Storage>,
    pub(super) dir: PathBuf,
    pub(super) options: TableOptions,
    pub(super) table: Arc<Memtable>,
    pub(super) flushed: u64,
}

impl Job {
    /// Writes the table file and its manifest edit; returns the file, or none when the table
    /// held neither a version nor a range delete (its sequences were claimed by batches that
    /// were never applied).
    pub(super) fn run(self, manifest: &mut ManifestStore) -> Result<Option<TableFile>> {
        let mut edit = ManifestEdit {
            flushed_sequence: Some(self.flushed),
            ..ManifestEdit::default()
        };
        let mut versions = self.table.iter();
        versions.seek_to_first();
        if versions.entry().is_none() && !self.table.has_tombstones() {
            manifest.append(&edit)?;
            return Ok(None);
        }
        let built = build(&self.table, self.options)?;
        let id = manifest.version().next_file();
        let path = table_path(&self.dir, id);
        let mut file = self.storage.create(&path)?;
        file.append(&built.bytes)?;
        file.sync()?;
        drop(file);
        self.storage.sync_dir(&self.dir)?;
        let meta = TableMeta {
            keyspace: 0,
            level: 0,
            id,
            size: u64::try_from(built.bytes.len()).map_err(|_| Error::FlushFailed)?,
            smallest: built.smallest,
            largest: built.largest,
            min_sequence: built.min_sequence,
            max_sequence: built.max_sequence,
        };
        edit.next_file = Some(id.checked_add(1).ok_or(Error::SequenceExhausted)?);
        edit.added.push(meta.clone());
        manifest.append(&edit)?;
        Ok(Some(TableFile {
            meta: Arc::new(meta),
            table: open(built.bytes, Some(&path))?,
        }))
    }
}

/// The file of table `id` in `dir`.
pub(super) fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.sst"))
}

/// How flushed tables of a keyspace are laid out.
pub(super) fn table_options(keyspace: &KeyspaceOptions) -> Result<TableOptions> {
    let invalid = |option| Error::InvalidOptions {
        option,
        reason: "does not fit this platform's address size".to_owned(),
    };
    Ok(TableOptions {
        block_bytes: usize::try_from(keyspace.block_bytes())
            .map_err(|_| invalid("keyspace.block_bytes"))?,
        restart_interval: usize::try_from(keyspace.block_restart_interval())
            .map_err(|_| invalid("keyspace.block_restart_interval"))?,
        bloom_bits_per_key: keyspace.bloom_bits_per_key(),
        compression: Compression::Lz4,
    })
}
