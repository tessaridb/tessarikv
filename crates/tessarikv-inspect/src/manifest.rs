//! A database's manifest, replayed: its store format, keyspaces and the table files of each level.

use std::io::Write;
use std::path::Path;

use tessarikv_format::{EndKind, FrameReader, KeyspaceRecord, LogRead, ManifestEdit, TableMeta};

use crate::error::{InspectError, Result, read};
use crate::text::escaped;

/// What the current manifest of a database describes.
#[derive(Debug, Default)]
pub struct Manifest {
    /// The store format its first edit records; `None` for a store from before formats.
    pub store_format: Option<u32>,
    /// The newest sequence its table files hold.
    pub flushed_sequence: u64,
    /// The next file number the database hands out.
    pub next_file: u64,
    /// The keyspaces beside the default one.
    pub keyspaces: Vec<KeyspaceRecord>,
    /// The table files it names.
    pub tables: Vec<TableMeta>,
}

/// Replays the current manifest of the database in `dir`. Encrypted databases are not read.
///
/// # Errors
///
/// [`InspectError::Read`] for a missing file, [`InspectError::Unreadable`] for a `CURRENT` or a
/// manifest that does not decode, encrypted ones included.
pub fn read_manifest(dir: &Path) -> Result<Manifest> {
    let current = dir.join("CURRENT");
    let unreadable = |path: &Path, detail: String| InspectError::Unreadable {
        path: path.to_path_buf(),
        what: "manifest",
        detail,
    };
    let named = read(&current)?;
    let name = std::str::from_utf8(&named)
        .ok()
        .and_then(|text| text.strip_suffix('\n'))
        .filter(|name| name.starts_with("MANIFEST-"))
        .ok_or_else(|| unreadable(&current, "it names no manifest".to_owned()))?;
    let number = name
        .trim_start_matches("MANIFEST-")
        .parse::<u64>()
        .map_err(|_| unreadable(&current, "it names no manifest".to_owned()))?;
    let path = dir.join(name);
    let bytes = read(&path)?;
    let mut manifest = Manifest::default();
    let mut frames = FrameReader::new(&bytes, number);
    loop {
        match frames.next_frame() {
            LogRead::Frame(payload) => {
                let edit = ManifestEdit::decode(payload).map_err(|error| {
                    unreadable(
                        &path,
                        format!("{error} (an encrypted database is not read)"),
                    )
                })?;
                manifest.apply(edit);
            }
            LogRead::End(end) if end.kind == EndKind::Clean => break,
            LogRead::End(end) => {
                let detail = format!(
                    "{:?} edit at offset {} (an encrypted database is not read)",
                    end.kind, end.offset
                );
                return Err(unreadable(&path, detail));
            }
        }
    }
    Ok(manifest)
}

impl Manifest {
    /// Applies one edit.
    fn apply(&mut self, edit: ManifestEdit) {
        if let Some(format) = edit.store_format {
            self.store_format = Some(format);
        }
        if let Some(flushed) = edit.flushed_sequence {
            self.flushed_sequence = flushed;
        }
        if let Some(next) = edit.next_file {
            self.next_file = next;
        }
        self.keyspaces
            .retain(|keyspace| !edit.dropped_keyspaces.contains(&keyspace.id));
        self.keyspaces.extend(edit.added_keyspaces);
        self.tables.retain(|table| {
            !edit.removed.iter().any(|removed| {
                removed.keyspace == table.keyspace
                    && removed.level == table.level
                    && removed.id == table.id
            })
        });
        self.tables.extend(edit.added);
    }

    /// Writes it to `out`, one line per fact.
    ///
    /// # Errors
    ///
    /// [`InspectError::Output`] when writing fails.
    pub fn print(&self, out: &mut dyn Write) -> Result<()> {
        match self.store_format {
            Some(format) => writeln!(out, "store format {format}")?,
            None => writeln!(out, "store format 0 (unrecorded)")?,
        }
        writeln!(out, "flushed sequence {}", self.flushed_sequence)?;
        writeln!(out, "next file {}", self.next_file)?;
        writeln!(out, "keyspace 0 default")?;
        for keyspace in &self.keyspaces {
            writeln!(
                out,
                "keyspace {} {}",
                keyspace.id,
                escaped(keyspace.name.as_bytes())
            )?;
        }
        let mut tables: Vec<&TableMeta> = self.tables.iter().collect();
        tables.sort_by_key(|table| (table.keyspace, table.level, table.id));
        for table in tables {
            writeln!(
                out,
                "table {} keyspace {} level {} bytes {} sequences {}..={} keys [{} .. {}]",
                table.id,
                table.keyspace,
                table.level,
                table.size,
                table.min_sequence,
                table.max_sequence,
                escaped(user_key(&table.smallest)),
                escaped(user_key(&table.largest)),
            )?;
        }
        Ok(())
    }
}

/// The user key of a stored key, without its trailer.
pub fn user_key(internal: &[u8]) -> &[u8] {
    tessarikv_types::InternalKeyRef::parse(internal).map_or(internal, |key| key.user_key())
}

#[cfg(test)]
pub mod tests;
