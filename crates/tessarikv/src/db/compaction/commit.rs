//! Recording a finished compaction: one synced manifest edit, then the view that reads it.

use std::sync::Arc;

use tessarikv_format::{ManifestEdit, TableMeta, TableRef};

use super::Compactions;
use super::pick::Pick;
use crate::db::flush_job::table_path;
use crate::db::installer::Flusher;
use crate::db::view::TableFile;
use crate::{CompactionCompleted, Error, KeyspaceId, Result};

impl Compactions {
    /// Appends the edit replacing `pick`'s tables of `keyspace` by `outputs` (by the moved table
    /// when there are none), then publishes the view that reads them.
    pub(super) async fn commit(
        &mut self,
        flusher: &mut Flusher,
        keyspace: u32,
        pick: &Pick,
        outputs: Vec<TableFile>,
    ) -> Result<()> {
        let moved = outputs.is_empty() && pick.is_move();
        let added: Vec<TableFile> = if moved {
            let current = flusher.view.load();
            let level = u8::try_from(pick.output_level).map_err(|_| Error::CompactionFailed)?;
            current
                .files(keyspace)
                .filter(|file| pick.inputs.iter().any(|input| input.id == file.meta.id))
                .map(|file| TableFile {
                    meta: Arc::new(TableMeta {
                        level,
                        ..(*file.meta).clone()
                    }),
                    ..file.clone()
                })
                .collect()
        } else {
            outputs
        };
        let edit = ManifestEdit {
            next_file: Some(flusher.numbers.bound()),
            added: added.iter().map(|file| (*file.meta).clone()).collect(),
            removed: pick
                .inputs
                .iter()
                .chain(&pick.overlaps)
                .map(|table| TableRef {
                    keyspace: table.keyspace,
                    level: table.level,
                    id: table.id,
                })
                .collect(),
            ..ManifestEdit::default()
        };
        let mut manifest = flusher.manifest.take().ok_or(Error::CompactionFailed)?;
        let (manifest, appended) = tokio::task::spawn_blocking(move || {
            let appended = manifest.append(&edit);
            (manifest, appended)
        })
        .await
        .map_err(|_| Error::CompactionFailed)?;
        flusher.manifest = Some(manifest);
        appended.map_err(|_| Error::CompactionFailed)?;
        let removed: Vec<u64> = pick
            .inputs
            .iter()
            .chain(&pick.overlaps)
            .map(|table| table.id)
            .collect();
        let current = flusher.view.load_full();
        if !moved {
            self.obsolete.extend(
                current
                    .files(keyspace)
                    .filter(|file| removed.contains(&file.meta.id))
                    .map(|file| {
                        (
                            Arc::downgrade(&file.blocks),
                            file.meta.id,
                            table_path(&flusher.dir, file.meta.id),
                        )
                    }),
            );
        }
        let inputs = || pick.inputs.iter().chain(&pick.overlaps);
        let completed = CompactionCompleted {
            keyspace: KeyspaceId::new(keyspace),
            output_level: u8::try_from(pick.output_level).unwrap_or(u8::MAX),
            input_tables: u32::try_from(removed.len()).unwrap_or(u32::MAX),
            output_tables: u32::try_from(added.len()).unwrap_or(u32::MAX),
            input_bytes: inputs().fold(0, |bytes: u64, table| bytes.saturating_add(table.size)),
            output_bytes: added
                .iter()
                .fold(0, |bytes: u64, file| bytes.saturating_add(file.meta.size)),
        };
        flusher.view.store(Arc::new(
            current.after_compaction(keyspace, &removed, added),
        ));
        if let Some(events) = &flusher.events {
            events.listener().compaction_completed(&completed);
        }
        Ok(())
    }
}
