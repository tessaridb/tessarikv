//! Values a compaction finds in blob files: carried as the index that names them, or read and
//! written again — into the job's own blob file — when their blob file is among the oldest the
//! keyspace empties, which is how a blob file's space comes back once nothing points into it.

use std::borrow::Cow;
use std::sync::Arc;

use tessarikv_types::{InternalKeyRef, ValueKind};

use super::output::Outputs;
use crate::db::tables::blobs::{BlobFile, locate};
use crate::db::view::TableFile;
use crate::{KeyspaceOptions, Result};

/// The blob files a job reads, and which of them it empties.
pub(crate) struct BlobInputs {
    /// Every blob file the inputs point into, by number.
    pub(crate) files: Vec<Arc<BlobFile>>,
    /// Values in blob files numbered below this are written again.
    pub(crate) gc_below: u64,
    /// Whether the keyspace's filter is asked about values, which are then read.
    pub(crate) filtered: bool,
}

/// A version as a job decides about it.
pub(super) struct Version<'a> {
    /// The kind the job sees: a value for an index it read.
    pub(super) kind: ValueKind,
    /// What the job sees: the value an index names, when it read it.
    pub(super) value: Cow<'a, [u8]>,
    /// The blob file whose index the job writes as it is.
    carry: Option<&'a Arc<BlobFile>>,
}

impl BlobInputs {
    /// The blob files `inputs` point into; the oldest `options`' cutoff share of those every
    /// table of the keyspace (`keyspace`) points into are emptied.
    pub(super) fn of<'f>(
        keyspace: impl Iterator<Item = &'f TableFile>,
        inputs: &[TableFile],
        options: &KeyspaceOptions,
    ) -> Self {
        let mut live: Vec<u64> = keyspace
            .flat_map(|file| file.blobs.iter().map(|blob| blob.id()))
            .collect();
        live.sort_unstable();
        live.dedup();
        let emptied = live
            .len()
            .saturating_mul(usize::try_from(options.blob_gc_age_cutoff_percent()).unwrap_or(0))
            / 100;
        let mut files: Vec<Arc<BlobFile>> = inputs
            .iter()
            .flat_map(|file| file.blobs.iter().cloned())
            .collect();
        files.sort_unstable_by_key(|blob| blob.id());
        files.dedup_by_key(|blob| blob.id());
        Self {
            files,
            gc_below: live.get(emptied).copied().unwrap_or(u64::MAX),
            filtered: options.compaction_filter().is_some(),
        }
    }

    /// Version `stored` of kind `kind` as the job decides about it. Runs on a blocking thread.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Corruption`] for an index naming no input blob file, and the errors of
    /// [`BlobFile::read_blocking`].
    pub(super) fn version<'a>(&'a self, kind: ValueKind, stored: &'a [u8]) -> Result<Version<'a>> {
        if kind != ValueKind::BlobIndex {
            return Ok(Version {
                kind,
                value: Cow::Borrowed(stored),
                carry: None,
            });
        }
        let (blob, handle) = locate(&self.files, stored)?;
        let emptied = blob.id() < self.gc_below;
        if !emptied && !self.filtered {
            return Ok(Version {
                kind,
                value: Cow::Borrowed(stored),
                carry: Some(blob),
            });
        }
        Ok(Version {
            kind: ValueKind::Value,
            value: Cow::Owned(blob.read_blocking(handle)?),
            carry: (!emptied).then_some(blob),
        })
    }
}

impl Version<'_> {
    /// Writes the version kept at `key` (`parsed`), stored as `stored`, to `outputs`.
    pub(super) fn write(
        &self,
        outputs: &mut Outputs<'_>,
        key: &[u8],
        parsed: &InternalKeyRef<'_>,
        stored: &[u8],
        starts_key: bool,
    ) -> Result<()> {
        match (self.carry, self.kind) {
            (Some(blob), _) => outputs.add_carried(key, starts_key, stored, blob),
            (None, ValueKind::Value) => outputs.add_value(key, parsed, starts_key, &self.value),
            (None, _) => outputs.add(key, starts_key, stored),
        }
    }
}
