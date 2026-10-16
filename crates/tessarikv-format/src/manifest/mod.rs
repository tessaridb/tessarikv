//! Manifest edits: the changes to the set of table files that a flush or a compaction makes,
//! recorded one per log frame so the set can be rebuilt after a crash.
//!
//! Format version 1, one edit per frame payload: a version byte, then fields, each a tag byte
//! followed by its value. Integers are LEB128 varints; keys are a varint length and the bytes.
//!
//! | Tag | Field | Value |
//! |---|---|---|
//! | 1 | flushed sequence | u64: every write at or below it is in a table file |
//! | 2 | next file | u64: the next file number to hand out |
//! | 3 | added table | keyspace u32, level u8, id u64, size u64, min and max sequence u64, smallest and largest internal key |
//! | 4 | removed table | keyspace u32, level u8, id u64 |
//! | 5 | added keyspace | id u32, name (UTF-8, varint length) |
//! | 6 | dropped keyspace | id u32 |
//! | 7 | next keyspace | u32: the next keyspace id to hand out |
//!
//! A scalar field appears at most once; tables and keyspaces may repeat. An unknown version or tag is refused,
//! never skipped: an edit this build does not understand cannot be applied safely. Lengths are
//! checked against the bytes left before anything is allocated. Integrity is the frame's
//! checksum's job; this layer refuses what a valid checksum cannot rule out.

#[cfg(test)]
mod tests;

use crate::error::{Error, Result};
use crate::varint::{get_u32, get_u64, put_u32, put_u64};

/// The format version written.
const VERSION: u8 = 1;

const FLUSHED_SEQUENCE: u8 = 1;
const NEXT_FILE: u8 = 2;
const ADDED_TABLE: u8 = 3;
const REMOVED_TABLE: u8 = 4;
const ADDED_KEYSPACE: u8 = 5;
const DROPPED_KEYSPACE: u8 = 6;
const NEXT_KEYSPACE: u8 = 7;

/// A keyspace as the manifest records it when it is added.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyspaceRecord {
    /// Its id, never reused.
    pub id: u32,
    /// Its name.
    pub name: String,
}

/// A table file as the manifest records it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableMeta {
    /// The keyspace the table belongs to.
    pub keyspace: u32,
    /// Its level.
    pub level: u8,
    /// Its file number.
    pub id: u64,
    /// Its size in bytes.
    pub size: u64,
    /// Its least internal key.
    pub smallest: Vec<u8>,
    /// Its greatest internal key.
    pub largest: Vec<u8>,
    /// The lowest sequence number it holds.
    pub min_sequence: u64,
    /// The highest sequence number it holds.
    pub max_sequence: u64,
}

/// A table file named by an edit that removes it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableRef {
    /// The keyspace the table belongs to.
    pub keyspace: u32,
    /// Its level.
    pub level: u8,
    /// Its file number.
    pub id: u64,
}

/// One change to the set of table files.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestEdit {
    /// Every write at or below this sequence is in a table file once the edit is applied.
    pub flushed_sequence: Option<u64>,
    /// The next file number to hand out.
    pub next_file: Option<u64>,
    /// Tables the edit adds.
    pub added: Vec<TableMeta>,
    /// Tables the edit removes.
    pub removed: Vec<TableRef>,
    /// Keyspaces the edit adds.
    pub added_keyspaces: Vec<KeyspaceRecord>,
    /// Keyspaces the edit drops, by id.
    pub dropped_keyspaces: Vec<u32>,
    /// The next keyspace id to hand out.
    pub next_keyspace: Option<u32>,
}

impl ManifestEdit {
    /// Appends the encoded edit to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(VERSION);
        if let Some(sequence) = self.flushed_sequence {
            out.push(FLUSHED_SEQUENCE);
            put_u64(out, sequence);
        }
        if let Some(file) = self.next_file {
            out.push(NEXT_FILE);
            put_u64(out, file);
        }
        for table in &self.added {
            out.push(ADDED_TABLE);
            put_u32(out, table.keyspace);
            out.push(table.level);
            for value in [table.id, table.size, table.min_sequence, table.max_sequence] {
                put_u64(out, value);
            }
            put_key(out, &table.smallest);
            put_key(out, &table.largest);
        }
        for table in &self.removed {
            out.push(REMOVED_TABLE);
            put_u32(out, table.keyspace);
            out.push(table.level);
            put_u64(out, table.id);
        }
        for keyspace in &self.added_keyspaces {
            out.push(ADDED_KEYSPACE);
            put_u32(out, keyspace.id);
            put_key(out, keyspace.name.as_bytes());
        }
        for id in &self.dropped_keyspaces {
            out.push(DROPPED_KEYSPACE);
            put_u32(out, *id);
        }
        if let Some(id) = self.next_keyspace {
            out.push(NEXT_KEYSPACE);
            put_u32(out, id);
        }
    }

    /// Decodes an edit.
    ///
    /// # Errors
    ///
    /// [`Error::UnsupportedVersion`] for another format version, [`Error::Malformed`] for an
    /// unknown or repeated field, and [`Error::Truncated`] when a field runs past the input.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut input = Input { bytes };
        let version = input.byte("manifest edit version")?;
        if version != VERSION {
            return Err(Error::UnsupportedVersion {
                what: "manifest edit",
                version,
            });
        }
        let mut edit = Self::default();
        while !input.bytes.is_empty() {
            match input.byte("manifest edit field")? {
                FLUSHED_SEQUENCE => set_once(&mut edit.flushed_sequence, input.u64()?)?,
                NEXT_FILE => set_once(&mut edit.next_file, input.u64()?)?,
                ADDED_TABLE => edit.added.push(TableMeta {
                    keyspace: input.u32()?,
                    level: input.byte("manifest edit level")?,
                    id: input.u64()?,
                    size: input.u64()?,
                    min_sequence: input.u64()?,
                    max_sequence: input.u64()?,
                    smallest: input.key()?,
                    largest: input.key()?,
                }),
                REMOVED_TABLE => edit.removed.push(TableRef {
                    keyspace: input.u32()?,
                    level: input.byte("manifest edit level")?,
                    id: input.u64()?,
                }),
                ADDED_KEYSPACE => edit.added_keyspaces.push(KeyspaceRecord {
                    id: input.u32()?,
                    name: String::from_utf8(input.key()?).map_err(|_| Error::Malformed {
                        what: "manifest edit",
                        reason: "a keyspace name is not UTF-8",
                    })?,
                }),
                DROPPED_KEYSPACE => edit.dropped_keyspaces.push(input.u32()?),
                NEXT_KEYSPACE => set_once(&mut edit.next_keyspace, input.u32()?)?,
                _ => {
                    return Err(Error::Malformed {
                        what: "manifest edit",
                        reason: "unknown field tag",
                    });
                }
            }
        }
        Ok(edit)
    }
}

fn put_key(out: &mut Vec<u8>, key: &[u8]) {
    // Keys longer than a u32 cannot be built: batch entries are bounded far below it.
    put_u32(out, u32::try_from(key.len()).unwrap_or(u32::MAX));
    out.extend_from_slice(key);
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> Result<()> {
    if slot.replace(value).is_some() {
        return Err(Error::Malformed {
            what: "manifest edit",
            reason: "a field given twice",
        });
    }
    Ok(())
}

/// The bytes of an edit not yet decoded.
struct Input<'a> {
    bytes: &'a [u8],
}

impl<'a> Input<'a> {
    fn byte(&mut self, what: &'static str) -> Result<u8> {
        let (&first, rest) = self.bytes.split_first().ok_or(Error::Truncated {
            what,
            needed: 1,
            available: 0,
        })?;
        self.bytes = rest;
        Ok(first)
    }

    fn u32(&mut self) -> Result<u32> {
        let (value, used) = get_u32(self.bytes).ok_or_else(|| self.bad_varint(5))?;
        self.advance(used);
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64> {
        let (value, used) = get_u64(self.bytes).ok_or_else(|| self.bad_varint(10))?;
        self.advance(used);
        Ok(value)
    }

    fn key(&mut self) -> Result<Vec<u8>> {
        let len = usize::try_from(self.u32()?).unwrap_or(usize::MAX);
        let (key, rest) = self
            .bytes
            .split_at_checked(len)
            .ok_or_else(|| Error::Truncated {
                what: "manifest edit key",
                needed: u64::try_from(len).unwrap_or(u64::MAX),
                available: u64::try_from(self.bytes.len()).unwrap_or(u64::MAX),
            })?;
        self.bytes = rest;
        Ok(key.to_vec())
    }

    fn advance(&mut self, used: usize) {
        self.bytes = self.bytes.get(used..).unwrap_or_default();
    }

    /// Why an integer of at most `max_bytes` bytes did not decode.
    fn bad_varint(&self, max_bytes: usize) -> Error {
        if self.bytes.len() >= max_bytes {
            return Error::Malformed {
                what: "manifest edit integer",
                reason: "longer than its type",
            };
        }
        Error::Truncated {
            what: "manifest edit integer",
            needed: 1,
            available: u64::try_from(self.bytes.len()).unwrap_or(u64::MAX),
        }
    }
}
