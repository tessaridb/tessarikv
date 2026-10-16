//! The options a database runs with, recorded beside its data on every open.
//!
//! Each open on storage writes `OPTIONS-<n>`, `n` the number of the manifest that open started,
//! so a later open's file sorts after an earlier one's. The file names every option, database-wide
//! and per keyspace, as the code holds them: it is a record for people and audits to read, not a
//! format to parse, and the engine never reads it back. It is written beside its final name,
//! synced, then renamed, so it is never seen half written; the two newest are kept. In an
//! encrypted database it is sealed like every other file: a seal header, then the text as one
//! sealed unit.

use std::path::Path;

use tessarikv_format::{FileKind, SEAL_HEADER_BYTES};

use crate::sealing::{Sealing, corruption};
use crate::storage::Storage;
use crate::{DbOptions, Result};

/// The start of every options file's name.
const PREFIX: &str = "OPTIONS-";
/// The end of an options file's name while it is being written.
const WRITING: &str = ".writing";
/// How many options files are kept.
const KEEP: usize = 2;

/// Records `options` as the options of the open that started manifest `number` in `dir`, and
/// removes older records past the newest [`KEEP`]. Runs on a blocking thread.
///
/// # Errors
///
/// The storage's error when the file cannot be written, synced, renamed or an old one removed.
pub(super) fn record(
    storage: &dyn Storage,
    dir: &Path,
    options: &DbOptions,
    number: u64,
    sealing: Option<&Sealing>,
) -> Result<()> {
    let name = format!("{PREFIX}{number:020}");
    let writing = dir.join(format!("{name}{WRITING}"));
    let text = format!(
        "# The options this database was opened with; for reading, not parsing.\n{options:#?}\n"
    );
    let mut file = storage.create(&writing)?;
    match sealing {
        Some(sealing) => {
            let (header, sealer) = sealing.new_file(FileKind::Options)?;
            let at = u64::try_from(SEAL_HEADER_BYTES).unwrap_or(u64::MAX);
            let sealed = sealer
                .seal(at, text.as_bytes())
                .map_err(|error| corruption(&writing, &error))?;
            file.append(&header.encode())?;
            file.append(&sealed)?;
        }
        None => file.append(text.as_bytes())?,
    }
    file.sync()?;
    drop(file);
    storage.rename(&writing, &dir.join(&name))?;
    storage.sync_dir(dir)?;
    let mut recorded = Vec::new();
    for name in storage.list(dir)? {
        if !name.starts_with(PREFIX) {
            continue;
        }
        if name.ends_with(WRITING) {
            // Left by an open that stopped while writing its record.
            storage.remove(&dir.join(&name))?;
        } else {
            recorded.push(name);
        }
    }
    recorded.sort_unstable();
    let older = recorded.len().saturating_sub(KEEP);
    for name in recorded.get(..older).unwrap_or_default() {
        storage.remove(&dir.join(name))?;
    }
    Ok(())
}
