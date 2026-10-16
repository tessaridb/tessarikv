//! A flush's blocking part: a table file per keyspace, synced, and the ONE manifest edit naming
//! them all with the flushed sequence, so a batch spanning keyspaces is in table files whole or
//! not at all.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tessarikv_format::{ManifestEdit, TableMeta, TableOptions};

use super::block_cache::BlockCache;
use super::table_blocks::open_table;
use super::tables::build;
use super::view::TableFile;
use crate::memtable::Memtable;
use crate::storage::Storage;
use crate::version::manifest::ManifestStore;
use crate::{Error, KeyspaceOptions, Result};

/// A flush's blocking part: the table files and the manifest edit.
pub(super) struct Job {
    pub(super) storage: Arc<dyn Storage>,
    pub(super) dir: PathBuf,
    /// The frozen tables, one per keyspace, with the layout each keyspace's tables take.
    pub(super) tables: Vec<(u32, TableOptions, Arc<Memtable>)>,
    pub(super) flushed: u64,
    pub(super) numbers: Arc<FileNumbers>,
    /// The cache the new table file reads its data blocks through.
    pub(super) cache: Arc<BlockCache>,
}

impl Job {
    /// Writes a table file for every table holding a version or a range delete (a table
    /// without either had its sequences claimed by batches never applied), then the manifest
    /// edit naming them; returns the files, one per keyspace written.
    pub(super) fn run(self, manifest: &mut ManifestStore) -> Result<Vec<TableFile>> {
        let mut edit = ManifestEdit {
            flushed_sequence: Some(self.flushed),
            ..ManifestEdit::default()
        };
        let mut written = Vec::new();
        for (keyspace, options, table) in &self.tables {
            let mut versions = table.iter();
            versions.seek_to_first();
            if versions.entry().is_none() && !table.has_tombstones() {
                continue;
            }
            let id = self.numbers.take()?;
            let path = table_path(&self.dir, id);
            let mut file = self.storage.create(&path)?;
            let built = build(table, *options, self.cache.sealing(), &mut |piece| {
                file.append(piece)
            })?;
            file.sync()?;
            drop(file);
            let meta = TableMeta {
                keyspace: *keyspace,
                level: 0,
                id,
                size: built.size(),
                smallest: built.smallest.clone(),
                largest: built.largest.clone(),
                min_sequence: built.min_sequence,
                max_sequence: built.max_sequence,
            };
            edit.added.push(meta.clone());
            written.push((meta, path, built));
        }
        if !written.is_empty() {
            self.storage.sync_dir(&self.dir)?;
            edit.next_file = Some(self.numbers.bound());
        }
        manifest.append(&edit)?;
        let mut files = Vec::with_capacity(written.len());
        for (meta, path, built) in written {
            // The metadata comes from the tail just written; data blocks from the file.
            let (table, blocks) = open_table(
                self.storage.as_ref(),
                &path,
                meta.id,
                &self.cache,
                Some((built.tail_offset, &built.tail)),
            )?;
            files.push(TableFile {
                meta: Arc::new(meta),
                table,
                blocks,
            });
        }
        Ok(files)
    }
}

/// Table file numbers, handed out once each to flushes and compactions that run side by side.
#[derive(Debug)]
pub(super) struct FileNumbers {
    next: AtomicU64,
}

impl FileNumbers {
    /// Numbers from `next` on.
    pub(super) fn new(next: u64) -> Self {
        Self {
            next: AtomicU64::new(next),
        }
    }

    /// A number no other table has had.
    ///
    /// # Errors
    ///
    /// [`Error::SequenceExhausted`] when the numbers have run out.
    pub(super) fn take(&self) -> Result<u64> {
        // Relaxed: uniqueness comes from the read-modify-write alone; nothing else is published.
        let mut next = self.next.load(Ordering::Relaxed);
        loop {
            let after = next.checked_add(1).ok_or(Error::SequenceExhausted)?;
            match self
                .next
                .compare_exchange_weak(next, after, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(taken) => return Ok(taken),
                Err(seen) => next = seen,
            }
        }
    }

    /// Above every number taken so far by this thread or by a job joined since: the next file
    /// number an edit records. Edits are appended one at a time, so the bound they record only
    /// grows.
    pub(super) fn bound(&self) -> u64 {
        // Relaxed: this thread's own takes, and those of joined jobs, are ordered before it.
        self.next.load(Ordering::Relaxed)
    }
}

/// The file of table `id` in `dir`.
pub(super) fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.sst"))
}

/// How the tables a keyspace writes to `level` are laid out.
pub(super) fn table_options(keyspace: &KeyspaceOptions, level: usize) -> Result<TableOptions> {
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
        compression: keyspace.compression_at(level),
    })
}
