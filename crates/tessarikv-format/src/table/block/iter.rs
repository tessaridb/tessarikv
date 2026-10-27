//! Walking and seeking a block.

use std::cmp::Ordering;
use std::ops::Range;

use super::Block;
use crate::error::{Error, Result};
use crate::varint;

/// A position in a [`Block`]: on an entry, or past the end.
///
/// The current key is rebuilt in a buffer the iterator owns, because an entry stores only the
/// suffix that differs from the previous key; the value is read in place from the block. A damaged
/// entry ends the iteration, and [`BlockIter::status`] then reports why.
#[derive(Debug)]
pub struct BlockIter {
    block: Block,
    /// Where the current entry starts.
    current: usize,
    /// Where the entry after the current one starts.
    next: usize,
    key: Vec<u8>,
    value: Range<usize>,
    valid: bool,
    error: Option<Error>,
}

impl BlockIter {
    pub(super) fn new(block: Block) -> Self {
        Self {
            block,
            current: 0,
            next: 0,
            key: Vec::new(),
            value: 0..0,
            valid: false,
            error: None,
        }
    }

    /// Whether the iterator is on an entry.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// The current key; empty when not on an entry.
    pub fn key(&self) -> &[u8] {
        if self.valid { &self.key } else { &[] }
    }

    /// The current value; empty when not on an entry.
    pub fn value(&self) -> &[u8] {
        if self.valid {
            self.block.bytes.get(self.value.clone()).unwrap_or_default()
        } else {
            &[]
        }
    }

    /// Why the iteration stopped early, if it did.
    ///
    /// # Errors
    ///
    /// The [`Error::Malformed`] describing the damaged entry.
    pub fn status(&self) -> Result<()> {
        self.error.clone().map_or(Ok(()), Err)
    }

    /// Moves to the first entry.
    pub fn seek_to_first(&mut self) {
        self.restart_at(0);
        self.step();
    }

    /// Moves to the first entry whose key is not less than `target`.
    pub fn seek(&mut self, target: &[u8]) {
        // The last restart point whose key is less than the target: its interval holds the
        // answer, or the answer is the first key of the next interval.
        let (mut low, mut high) = (0, self.block.restart_count.saturating_sub(1));
        while low < high {
            let middle = low.saturating_add(high.saturating_sub(low).saturating_add(1) / 2);
            let compare = self.block.compare;
            // A restart entry holds its whole key: compare it where it lies, without copying.
            let order = match self.restart_key(middle) {
                Some(key) => compare(key, target),
                None => {
                    self.restart_at(middle);
                    self.step();
                    if !self.valid {
                        return;
                    }
                    compare(&self.key, target)
                }
            };
            if order == Ordering::Less {
                low = middle;
            } else {
                high = middle.saturating_sub(1);
            }
        }
        self.restart_at(low);
        self.step();
        while self.valid && (self.block.compare)(&self.key, target) == Ordering::Less {
            self.step();
        }
    }

    /// Moves to the next entry; past the last one the iterator is no longer valid.
    pub fn next(&mut self) {
        if self.valid {
            self.step();
        }
    }

    /// Moves to the last entry.
    pub fn seek_to_last(&mut self) {
        self.restart_at(self.block.restart_count.saturating_sub(1));
        self.step();
        while self.valid && self.next < self.block.entries_end {
            self.step();
        }
    }

    /// Moves to the previous entry; before the first one the iterator is no longer valid.
    ///
    /// Entries are prefix-compressed forward, so the previous one is found by decoding forward
    /// from the last restart point before the current entry.
    pub fn prev(&mut self) {
        if !self.valid {
            return;
        }
        let current = self.current;
        let Some(restart) = self.restart_before(current) else {
            self.key.clear();
            self.valid = false;
            self.next = self.block.entries_end;
            return;
        };
        self.restart_at(restart);
        self.step();
        while self.valid && self.next < current {
            self.step();
        }
    }

    /// The last restart point whose entry starts before `offset`.
    fn restart_before(&self, offset: usize) -> Option<usize> {
        // The first restart point at or after `offset`, by binary search; the one before it.
        let (mut low, mut high) = (0, self.block.restart_count);
        while low < high {
            let middle = low.saturating_add(high.saturating_sub(low) / 2);
            match self.block.restart(middle) {
                Some(start) if start < offset => low = middle.saturating_add(1),
                _ => high = middle,
            }
        }
        low.checked_sub(1)
    }

    /// Positions before the entry at restart point `index`, with no previous key.
    fn restart_at(&mut self, index: usize) {
        self.key.clear();
        self.valid = false;
        match self.block.restart(index) {
            Some(offset) if index < self.block.restart_count => self.next = offset,
            _ => self.next = self.block.entries_end,
        }
    }

    /// The key of restart entry `index`, read in place; `None` when the entry does not decode
    /// as a restart entry (the caller then decodes it the usual way, which reports why).
    fn restart_key(&self, index: usize) -> Option<&[u8]> {
        let entries = self.block.bytes.get(..self.block.entries_end)?;
        let mut at = self.block.restart(index)?;
        let mut field = || -> Option<usize> {
            let (value, used) = varint::get_u32(entries.get(at..)?)?;
            at = at.checked_add(used)?;
            usize::try_from(value).ok()
        };
        let shared = field()?;
        let unshared = field()?;
        let _value_len = field()?;
        if shared != 0 {
            return None;
        }
        entries.get(at..at.checked_add(unshared)?)
    }

    /// Decodes the entry at `self.next` into the current position.
    fn step(&mut self) {
        if self.next >= self.block.entries_end {
            self.valid = false;
            return;
        }
        self.current = self.next;
        match self.decode() {
            Some(next) => {
                self.next = next;
                self.valid = true;
            }
            None => {
                self.valid = false;
                self.next = self.block.entries_end;
                self.error = Some(Error::Malformed {
                    what: "block",
                    reason: "an entry runs past the entries or shares more than the previous key",
                });
            }
        }
    }

    /// Reads the entry at `self.next`, updating the key and value; returns where the following
    /// entry starts.
    fn decode(&mut self) -> Option<usize> {
        let entries = self.block.bytes.get(..self.block.entries_end)?;
        let mut at = self.next;
        let field = |at: &mut usize| -> Option<usize> {
            let (value, used) = varint::get_u32(entries.get(*at..)?)?;
            *at = at.checked_add(used)?;
            usize::try_from(value).ok()
        };
        let shared = field(&mut at)?;
        let unshared = field(&mut at)?;
        let value_len = field(&mut at)?;
        if shared > self.key.len() {
            return None;
        }
        let suffix_end = at.checked_add(unshared)?;
        let value_end = suffix_end.checked_add(value_len)?;
        let suffix = entries.get(at..suffix_end)?;
        entries.get(suffix_end..value_end)?;
        self.key.truncate(shared);
        self.key.extend_from_slice(suffix);
        self.value = suffix_end..value_end;
        Some(value_end)
    }
}
