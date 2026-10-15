//! The manifest on storage: the edit log a version is rebuilt from.
//!
//! `MANIFEST-<n>` holds one checksummed frame per edit, tagged with `n`. `CURRENT` names the
//! manifest in use. A new manifest starts with one edit describing a whole version, is synced,
//! and only then made current: `CURRENT` is written to a temporary file, synced, renamed over the
//! old one and the directory synced, so a crash leaves either the old manifest or the new one
//! current, never a half-written name. The previous manifest is removed after the switch.
//!
//! An edit is appended and synced before it counts: a writer installs nothing it has not made
//! durable here first. An edit that would contradict the version is refused before it is written.
//! On recovery the valid prefix of the manifest is applied; a torn last frame was never
//! acknowledged and is dropped. Opening always moves to a new manifest, so nothing is ever
//! appended after a torn frame.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_format::{FrameReader, LogRead, ManifestEdit, encode_frame, frame_bytes};

use super::Version;
use crate::storage::{Storage, WritableFile, read_all};
use crate::{Error, Result};

/// The file naming the manifest in use.
pub(crate) const CURRENT: &str = "CURRENT";

/// `CURRENT` while it is being replaced.
const CURRENT_TEMPORARY: &str = "CURRENT.tmp";

/// The prefix of a manifest's file name; twenty decimal digits follow.
const MANIFEST_PREFIX: &str = "MANIFEST-";

/// The manifest in use and the version it describes. Owned by one writer.
pub(crate) struct ManifestStore {
    number: u64,
    file: Box<dyn WritableFile>,
    version: Arc<Version>,
}

impl ManifestStore {
    /// Starts manifest `number` in `dir` holding `version`, makes it current, and removes the
    /// manifest it replaces. Runs on a blocking thread.
    ///
    /// # Errors
    ///
    /// Whatever storage returns; on an error the previous manifest stays current.
    pub(crate) fn create(
        storage: Arc<dyn Storage>,
        dir: &Path,
        version: &Version,
        number: u64,
    ) -> Result<Self> {
        let previous = read_current(storage.as_ref(), dir)?;
        let mut file = storage.create(&manifest_path(dir, number))?;
        append_edit(file.as_mut(), number, &version.as_edit())?;
        let temporary = dir.join(CURRENT_TEMPORARY);
        let mut current = storage.create(&temporary)?;
        current.append(format!("{}\n", manifest_name(number)).as_bytes())?;
        current.sync()?;
        drop(current);
        storage.rename(&temporary, &dir.join(CURRENT))?;
        storage.sync_dir(dir)?;
        if let Some(previous) = previous.filter(|previous| *previous != number) {
            storage.remove(&manifest_path(dir, previous))?;
        }
        Ok(Self {
            number,
            file,
            version: Arc::new(version.clone()),
        })
    }

    /// The version as of the last edit appended.
    pub(crate) fn version(&self) -> Arc<Version> {
        Arc::clone(&self.version)
    }

    /// Applies `edit` to the version, appends it and syncs it. Runs on a blocking thread.
    ///
    /// # Errors
    ///
    /// [`Error::Corruption`] when the edit contradicts the version (nothing is written), and
    /// whatever storage returns; after a storage error the manifest may or may not hold the edit,
    /// so its writer must stop installing.
    pub(crate) fn append(&mut self, edit: &ManifestEdit) -> Result<()> {
        let next = self.version.apply(edit)?;
        append_edit(self.file.as_mut(), self.number, edit)?;
        self.version = Arc::new(next);
        Ok(())
    }

    /// The version the current manifest in `dir` describes, and its number; `None` when `dir`
    /// holds no manifest. Runs on a blocking thread.
    ///
    /// # Errors
    ///
    /// [`Error::Corruption`] when `CURRENT` or an edit is damaged or contradicts the version,
    /// [`Error::NotFound`] when the manifest `CURRENT` names is missing, and whatever storage
    /// returns.
    pub(crate) fn recover(storage: &dyn Storage, dir: &Path) -> Result<Option<(Version, u64)>> {
        let Some(number) = read_current(storage, dir)? else {
            return Ok(None);
        };
        let path = manifest_path(dir, number);
        let bytes = read_all(storage, &path, "manifest")?;
        let mut frames = FrameReader::new(&bytes, number);
        let mut version = Version::empty();
        while let LogRead::Frame(payload) = frames.next_frame() {
            let edit = ManifestEdit::decode(payload).map_err(|error| Error::Corruption {
                what: "manifest",
                path: Some(path.clone()),
                offset: None,
                detail: error.to_string(),
            })?;
            version = version.apply(&edit).map_err(|error| match error {
                Error::Corruption { detail, .. } => Error::Corruption {
                    what: "manifest",
                    path: Some(path.clone()),
                    offset: None,
                    detail,
                },
                other => other,
            })?;
        }
        Ok(Some((version, number)))
    }
}

/// Frames `edit` as a record of manifest `number`, appends it and syncs.
fn append_edit(file: &mut dyn WritableFile, number: u64, edit: &ManifestEdit) -> Result<()> {
    let mut payload = Vec::new();
    edit.encode(&mut payload);
    // A manifest edit is far below the frame limit: it holds a few table descriptions.
    let too_large = |_| Error::Unsupported {
        what: "a manifest edit larger than a log frame holds",
    };
    let mut frame = vec![0; frame_bytes(payload.len()).map_err(too_large)?];
    encode_frame(number, &payload, &mut frame).map_err(too_large)?;
    file.append(&frame)?;
    file.sync()
}

/// The manifest number `CURRENT` names; `None` when there is no `CURRENT`.
fn read_current(storage: &dyn Storage, dir: &Path) -> Result<Option<u64>> {
    let path = dir.join(CURRENT);
    if !storage.exists(&path)? {
        return Ok(None);
    }
    let bytes = read_all(storage, &path, "manifest")?;
    let number = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|text| text.strip_suffix('\n'))
        .and_then(|name| name.strip_prefix(MANIFEST_PREFIX))
        .filter(|digits| digits.len() == 20 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|digits| digits.parse::<u64>().ok())
        .ok_or_else(|| Error::Corruption {
            what: "manifest",
            path: Some(path.clone()),
            offset: Some(0),
            detail: "CURRENT does not name a manifest".to_owned(),
        })?;
    Ok(Some(number))
}

fn manifest_name(number: u64) -> String {
    format!("{MANIFEST_PREFIX}{number:020}")
}

fn manifest_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(manifest_name(number))
}
