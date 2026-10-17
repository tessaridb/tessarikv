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
//!
//! In an encrypted database a manifest starts with a seal header of its own, and each edit is
//! sealed as the unit at the offset of its frame; an edit failing authentication under a checksum
//! that holds was changed, and recovery fails.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::STORE_FORMAT;
use tessarikv_format::{
    FRAME_HEADER_BYTES, FileKind, FileSealer, FrameReader, LogRead, ManifestEdit,
    SEAL_HEADER_BYTES, encode_frame, frame_bytes,
};

use super::Version;
use super::store_format::check_store_format;
use crate::sealing::Sealing;
use crate::storage::{Storage, WritableFile, read_all};
use crate::{Error, Result};

/// The file naming the manifest in use.
pub(crate) const CURRENT: &str = "CURRENT";

/// `CURRENT` while it is being replaced.
pub(crate) const CURRENT_TEMPORARY: &str = "CURRENT.tmp";

/// The prefix of a manifest's file name; twenty decimal digits follow.
const MANIFEST_PREFIX: &str = "MANIFEST-";

/// The manifest in use and the version it describes. Owned by one writer.
pub(crate) struct ManifestStore {
    number: u64,
    file: Box<dyn WritableFile>,
    version: Arc<Version>,
    /// The manifest's key, when the database is encrypted.
    sealer: Option<Arc<FileSealer>>,
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    /// Bytes past which the manifest is replaced by a new one holding the version as one edit.
    limit: u64,
    /// How a new manifest is encrypted, when the database is.
    sealing: Option<Arc<Sealing>>,
}

impl ManifestStore {
    /// Starts manifest `number` in `dir` holding `version`, encrypted under a key of its own
    /// when there is `sealing`, makes it current, and removes the manifest it replaces. Runs on a
    /// blocking thread.
    ///
    /// # Errors
    ///
    /// Whatever storage returns; on an error the previous manifest stays current.
    pub(crate) fn create(
        storage: Arc<dyn Storage>,
        dir: &Path,
        version: &Version,
        number: u64,
        sealing: Option<&Sealing>,
    ) -> Result<Self> {
        let previous = read_current(storage.as_ref(), dir)?;
        let (file, sealer) = start(storage.as_ref(), dir, version, number, sealing)?;
        if let Some(previous) = previous.filter(|previous| *previous != number) {
            storage.remove(&manifest_path(dir, previous))?;
        }
        Ok(Self {
            number,
            file,
            version: Arc::new(version.clone()),
            sealer,
            storage,
            dir: dir.to_path_buf(),
            limit: u64::MAX,
            sealing: None,
        })
    }

    /// Replaces the manifest by a new one, encrypted with `sealing` when there is one, once an
    /// append leaves it past `limit` bytes.
    pub(crate) fn rolling_at(mut self, limit: u64, sealing: Option<Arc<Sealing>>) -> Self {
        self.limit = limit;
        self.sealing = sealing;
        self
    }

    /// Starts the next manifest holding the version as one edit and makes it current, then lets
    /// go of this one and removes it: a crash leaves `CURRENT` naming one whole manifest or the
    /// other. Runs on a blocking thread.
    fn roll(&mut self) -> Result<()> {
        let next = self.number.checked_add(1).ok_or(Error::SequenceExhausted)?;
        let sealing = self.sealing.as_deref();
        let (file, sealer) = start(
            self.storage.as_ref(),
            &self.dir,
            &self.version,
            next,
            sealing,
        )?;
        // The old handle goes first: a file still open cannot be removed everywhere.
        drop(std::mem::replace(&mut self.file, file));
        let previous = std::mem::replace(&mut self.number, next);
        self.sealer = sealer;
        self.storage.remove(&manifest_path(&self.dir, previous))
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
        append_edit(
            self.file.as_mut(),
            self.number,
            edit,
            self.sealer.as_deref(),
        )?;
        self.version = Arc::new(next);
        if self.file.len() > self.limit {
            self.roll()?;
        }
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
    pub(crate) fn recover(
        storage: &dyn Storage,
        dir: &Path,
        sealing: Option<&Sealing>,
    ) -> Result<Option<(Version, u64)>> {
        let Some(number) = read_current(storage, dir)? else {
            return Ok(None);
        };
        let path = manifest_path(dir, number);
        let bytes = read_all(storage, &path, "manifest")?;
        let (start, sealer) = match sealing {
            Some(sealing) => (
                SEAL_HEADER_BYTES,
                Some(sealing.open_file(&bytes, FileKind::Manifest, &path)?),
            ),
            None => (0, None),
        };
        let mut frames = FrameReader::new(bytes.get(start..).unwrap_or_default(), number);
        let mut version = Version::empty();
        let mut at = u64::try_from(start).unwrap_or(u64::MAX);
        let mut first = true;
        while let LogRead::Frame(payload) = frames.next_frame() {
            let opened = match &sealer {
                Some(sealer) => Some(sealer.open(at, payload).map_err(|_| Error::Corruption {
                    what: "manifest",
                    path: Some(path.clone()),
                    offset: Some(at),
                    detail: "an edit fails authentication".to_owned(),
                })?),
                None => None,
            };
            let frame = payload.len().saturating_add(FRAME_HEADER_BYTES);
            at = at.saturating_add(u64::try_from(frame).unwrap_or(u64::MAX));
            let payload = opened.as_deref().unwrap_or(payload);
            if std::mem::take(&mut first) {
                check_store_format(payload, &path)?;
            }
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
        if first {
            // Every manifest is written with its first edit before it is made current, so one
            // with none is damaged, never a store with nothing in it.
            return Err(Error::Corruption {
                what: "manifest",
                path: Some(path),
                offset: None,
                detail: "the manifest holds no edit".to_owned(),
            });
        }
        Ok(Some((version, number)))
    }
}

/// A manifest just written and made current: open for appends, with its key when encrypted.
type Started = (Box<dyn WritableFile>, Option<Arc<FileSealer>>);

/// Writes manifest `number` in `dir` holding `version` as one edit, encrypted under a key of its
/// own when there is `sealing`, synced, and makes it current; returns it open for appends.
fn start(
    storage: &dyn Storage,
    dir: &Path,
    version: &Version,
    number: u64,
    sealing: Option<&Sealing>,
) -> Result<Started> {
    let mut file = storage.create(&manifest_path(dir, number))?;
    let sealer = match sealing {
        Some(sealing) => {
            let (header, sealer) = sealing.new_file(FileKind::Manifest)?;
            file.append(&header.encode())?;
            Some(sealer)
        }
        None => None,
    };
    let mut snapshot = version.as_edit();
    snapshot.store_format = Some(STORE_FORMAT);
    append_edit(file.as_mut(), number, &snapshot, sealer.as_deref())?;
    let temporary = dir.join(CURRENT_TEMPORARY);
    let mut current = storage.create(&temporary)?;
    current.append(format!("{}\n", manifest_name(number)).as_bytes())?;
    current.sync()?;
    drop(current);
    storage.rename(&temporary, &dir.join(CURRENT))?;
    storage.sync_dir(dir)?;
    Ok((file, sealer))
}

/// Frames `edit` as a record of manifest `number`, sealed as the unit at the frame's offset
/// when the manifest has a `sealer`, appends it and syncs.
fn append_edit(
    file: &mut dyn WritableFile,
    number: u64,
    edit: &ManifestEdit,
    sealer: Option<&FileSealer>,
) -> Result<()> {
    let mut payload = Vec::new();
    edit.encode(&mut payload);
    if let Some(sealer) = sealer {
        payload = sealer
            .seal(file.len(), &payload)
            .map_err(|_| Error::Unsupported {
                what: "a manifest edit larger than a sealed unit",
            })?;
    }
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
