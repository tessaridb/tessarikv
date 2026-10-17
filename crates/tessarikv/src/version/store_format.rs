//! The store format gate: a database written by a later release is refused at open, before its
//! tables or logs are read.

use std::path::Path;

use tessarikv_constants::STORE_FORMAT;
use tessarikv_format::peek_store_format;

use crate::{Error, Result};

/// Refuses a manifest whose first edit records a store format this build does not read, before
/// any edit is decoded: a later release may have changed what follows the fixed prefix.
pub(super) fn check_store_format(first: &[u8], path: &Path) -> Result<()> {
    let found = peek_store_format(first)
        .map_err(|error| Error::Corruption {
            what: "manifest",
            path: Some(path.to_path_buf()),
            offset: None,
            detail: error.to_string(),
        })?
        .unwrap_or(0);
    if found > STORE_FORMAT {
        return Err(Error::UnsupportedFormat {
            path: path.to_path_buf(),
            found,
            newest: STORE_FORMAT,
        });
    }
    Ok(())
}
