//! Recording a finished compaction: one synced manifest edit, then the view that reads it.

use std::sync::Arc;

use tessarikv_format::{ManifestEdit, TableMeta, TableRef};

use super::pick::Pick;
use super::{Compactions, KEYSPACE};
use crate::db::flush_job::table_path;
use crate::db::installer::Flusher;
use crate::db::view::TableFile;
use crate::{Error, Result};

impl Compactions {
    /// Appends the edit replacing `pick`'s tables by `outputs` (by the moved table when there
    /// are none), then publishes the view that reads them.
    pub(super) async fn commit(
        &mut self,
        flusher: &mut Flusher,
        pick: &Pick,
        outputs: Vec<TableFile>,
    ) -> Result<()> {
        let moved = outputs.is_empty() && pick.is_move();
        let added: Vec<TableFile> = if moved {
            let current = flusher.view.load();
            let level = u8::try_from(pick.output_level).map_err(|_| Error::CompactionFailed)?;
            current
                .files(KEYSPACE)
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
                    .files(KEYSPACE)
                    .filter(|file| removed.contains(&file.meta.id))
                    .map(|file| {
                        (
                            Arc::downgrade(&file.blocks),
                            table_path(&flusher.dir, file.meta.id),
                        )
                    }),
            );
        }
        flusher.view.store(Arc::new(
            current.after_compaction(KEYSPACE, &removed, added),
        ));
        Ok(())
    }
}
