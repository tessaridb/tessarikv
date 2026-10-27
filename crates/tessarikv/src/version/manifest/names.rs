//! The names of the manifest's files: `MANIFEST-<n>` and `CURRENT`, which names the one in use.

use std::path::{Path, PathBuf};

use crate::storage::{Storage, read_all};
use crate::{Error, Result};

/// The file naming the manifest in use.
pub(crate) const CURRENT: &str = "CURRENT";

/// `CURRENT` while it is being replaced.
pub(crate) const CURRENT_TEMPORARY: &str = "CURRENT.tmp";

/// The prefix of a manifest's file name; twenty decimal digits follow.
const MANIFEST_PREFIX: &str = "MANIFEST-";

/// The manifest number `CURRENT` names; `None` when there is no `CURRENT`.
pub(super) fn read_current(storage: &dyn Storage, dir: &Path) -> Result<Option<u64>> {
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

pub(super) fn manifest_name(number: u64) -> String {
    format!("{MANIFEST_PREFIX}{number:020}")
}

pub(super) fn manifest_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(manifest_name(number))
}
