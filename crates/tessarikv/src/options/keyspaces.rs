//! Named keyspaces a database opens beside the default one.
//!
//! A database on storage records which keyspaces it holds. Opening it must name every one of
//! them; a keyspace the options name and the database does not hold is added when
//! `create_missing_keyspaces` is set and refused otherwise.

use tessarikv_constants as limits;

use super::{DbOptions, KeyspaceOptions};
use crate::{Error, Result};

/// The name the default keyspace answers to; no other keyspace may take it.
pub(crate) const DEFAULT_NAME: &str = "default";

impl DbOptions {
    /// The named keyspaces beside the default one, in the order given.
    #[must_use]
    pub fn keyspaces(&self) -> &[(String, KeyspaceOptions)] {
        &self.keyspaces
    }

    /// Whether opening adds the keyspaces the options name and the database does not hold.
    /// Off by default.
    #[must_use]
    pub const fn create_missing_keyspaces(&self) -> bool {
        self.create_missing_keyspaces
    }
}

impl super::DbOptionsBuilder {
    /// Names a keyspace to open beside the default one, with its options. Options are not
    /// recorded with the database, so every open names every keyspace it holds.
    pub fn keyspace(mut self, name: impl Into<String>, options: KeyspaceOptions) -> Self {
        self.options.keyspaces.push((name.into(), options));
        self
    }

    /// Sets [`DbOptions::create_missing_keyspaces`].
    pub const fn create_missing_keyspaces(mut self, value: bool) -> Self {
        self.options.create_missing_keyspaces = value;
        self
    }
}

/// Refuses an empty, overlong, reserved or repeated name, and more keyspaces than a database
/// holds.
pub(super) fn check(options: &DbOptions) -> Result<()> {
    let refuse = |reason: &str| {
        Err(Error::InvalidOptions {
            option: "db.keyspaces",
            reason: reason.to_owned(),
        })
    };
    let most = usize::try_from(limits::MAX_KEYSPACES).unwrap_or(usize::MAX);
    // The default keyspace counts too.
    if options.keyspaces.len() >= most {
        return refuse("more keyspaces than a database holds");
    }
    for (at, (name, _)) in options.keyspaces.iter().enumerate() {
        if name.is_empty() || name.len() > limits::MAX_KEYSPACE_NAME_BYTES {
            return refuse("a keyspace name is empty or too long");
        }
        if name == DEFAULT_NAME {
            return refuse("a keyspace takes the default keyspace's name");
        }
        if options
            .keyspaces
            .iter()
            .skip(at.saturating_add(1))
            .any(|(other, _)| other == name)
        {
            return refuse("a keyspace is named twice");
        }
    }
    Ok(())
}
