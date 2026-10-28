//! A whole-key bloom filter, blocked by cache line: a lookup touches one 64-byte line.
//!
//! ```text
//! [line: 64 bytes]*  [probes: u8]  [line count: u32]
//! ```
//!
//! A key's 64-bit hash picks one line from its high half, and its probes are bits of that line
//! picked from its low half, so a lookup costs one cache miss however many probes it makes. Each
//! probe remixes the low half and takes its top nine bits; tables written before format version
//! 3 stepped through the line by double hashing instead, which correlates the probes and keeps
//! extra bits per key from lowering the false-positive rate, and are still read that way. A filter answers only "certainly absent" or "possibly present": a key that
//! was added always tests present.
//!
//! The hash is part of the format: changing it would make existing filters reject keys they hold.

#[cfg(test)]
mod tests;

mod kind;
mod ribbon;

pub use kind::FilterPolicy;
pub(crate) use kind::{FilterBuilder, TableFilter};

use crate::error::{Error, Result};

/// Bytes of one line.
const LINE_BYTES: usize = 64;

/// Bits of one line.
const LINE_BITS: u32 = 512;

/// Bytes after the lines: the probe count and the line count.
const TRAILER_BYTES: usize = 5;

/// The most probes a filter may make.
const MAX_PROBES: u8 = 30;

/// An odd multiplier near 2^32 / phi: each multiplication spreads the low bits into the top ones.
const PROBE_MIX: u32 = 0x9e37_79b9;

/// Shift keeping the top nine bits of a remix: a bit of the 512-bit line.
const PROBE_SHIFT: u32 = 23;

/// How a filter picks the bits of a key within its line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Probing {
    /// Double hashing: a start and a step from the low half (format versions 1 and 2).
    Stepped,
    /// Successive multiplicative remixes of the low half (format version 3).
    Remixed,
}

/// Collects the hashes of the keys of one table, then lays out its filter.
#[derive(Debug)]
pub struct BloomBuilder {
    bits_per_key: u32,
    hashes: Vec<u64>,
}

impl BloomBuilder {
    /// A builder spending `bits_per_key` bits per key (at least one). Ten bits per key lets
    /// about one absent key in a hundred through.
    pub fn new(bits_per_key: u32) -> Self {
        Self {
            bits_per_key: bits_per_key.max(1),
            hashes: Vec::new(),
        }
    }

    /// Records `key`.
    pub fn add(&mut self, key: &[u8]) {
        self.hashes.push(hash(key));
    }

    /// The filter of every key added; the builder is empty again afterwards.
    pub fn finish(&mut self) -> Vec<u8> {
        self.finish_probing(Probing::Remixed)
    }

    /// As [`BloomBuilder::finish`], picking each key's bits by `probing`.
    pub(crate) fn finish_probing(&mut self, probing: Probing) -> Vec<u8> {
        let keys = u64::try_from(self.hashes.len()).unwrap_or(u64::MAX);
        let bits = keys.saturating_mul(u64::from(self.bits_per_key));
        let lines = bits
            .div_ceil(u64::from(LINE_BITS))
            .clamp(1, u64::from(u32::MAX));
        let line_count = u32::try_from(lines).unwrap_or(u32::MAX);
        let probes = probes_for(self.bits_per_key);
        let size = usize::try_from(line_count)
            .unwrap_or(usize::MAX)
            .saturating_mul(LINE_BYTES);
        let mut filter = vec![0_u8; size];
        for key_hash in self.hashes.drain(..) {
            let line = line_of(key_hash, line_count);
            for bit in bits_of(key_hash, probes, probing) {
                set(&mut filter, line, bit);
            }
        }
        filter.push(probes);
        filter.extend_from_slice(&line_count.to_le_bytes());
        filter
    }
}

/// A decoded filter.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bytes: Vec<u8>,
    line_count: u32,
    probes: u8,
    probing: Probing,
}

impl BloomFilter {
    /// Decodes `bytes` as a filter [`BloomBuilder`] wrote.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] for fewer bytes than a trailer; [`Error::Malformed`] when the line
    /// count does not match the bytes or the probe count is out of range.
    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        Self::with_probing(bytes, Probing::Remixed)
    }

    /// Decodes `bytes` as a filter whose bits were picked by `probing`.
    pub(crate) fn with_probing(bytes: Vec<u8>, probing: Probing) -> Result<Self> {
        let malformed = |reason| Error::Malformed {
            what: "filter",
            reason,
        };
        let Some(lines_end) = bytes.len().checked_sub(TRAILER_BYTES) else {
            return Err(Error::Truncated {
                what: "filter",
                needed: 5,
                available: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            });
        };
        let probes = *bytes.get(lines_end).ok_or(malformed("trailer"))?;
        let count = bytes
            .get(lines_end.saturating_add(1)..)
            .and_then(|field| field.try_into().ok())
            .map(u32::from_le_bytes)
            .ok_or(malformed("trailer"))?;
        if probes == 0 || probes > MAX_PROBES {
            return Err(malformed("probe count out of range"));
        }
        let expected = usize::try_from(count)
            .ok()
            .and_then(|lines| lines.checked_mul(LINE_BYTES));
        if count == 0 || expected != Some(lines_end) {
            return Err(malformed("line count does not match the filter's size"));
        }
        Ok(Self {
            bytes,
            line_count: count,
            probes,
            probing,
        })
    }

    /// Whether `key` may be in the table: `false` means it certainly is not.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let key_hash = hash(key);
        let line = line_of(key_hash, self.line_count);
        bits_of(key_hash, self.probes, self.probing).all(|bit| is_set(&self.bytes, line, bit))
    }
}

/// Probes for `bits_per_key`: about ln 2 per bit, which minimises false positives.
fn probes_for(bits_per_key: u32) -> u8 {
    let probes = bits_per_key.saturating_mul(69).saturating_add(50) / 100;
    u8::try_from(probes.clamp(1, u32::from(MAX_PROBES))).unwrap_or(MAX_PROBES)
}

/// The line of a key: the high half of its hash, scaled to the line count.
fn line_of(key_hash: u64, line_count: u32) -> usize {
    let high = key_hash.checked_shr(32).unwrap_or(0);
    let line = high
        .saturating_mul(u64::from(line_count))
        .checked_shr(32)
        .unwrap_or(0);
    usize::try_from(line).unwrap_or(0)
}

/// The bits of a key within its line, picked from the low half of its hash by `probing`.
fn bits_of(key_hash: u64, probes: u8, probing: Probing) -> impl Iterator<Item = u32> {
    let low = u32::try_from(key_hash & 0xFFFF_FFFF).unwrap_or(0);
    let step = low.rotate_right(17) | 1;
    let mut mixed = low;
    (0..u32::from(probes)).map(move |probe| match probing {
        Probing::Stepped => low.wrapping_add(probe.wrapping_mul(step)) % LINE_BITS,
        Probing::Remixed => {
            mixed = mixed.wrapping_mul(PROBE_MIX);
            mixed >> PROBE_SHIFT
        }
    })
}

fn set(filter: &mut [u8], line: usize, bit: u32) {
    if let Some(byte) = byte_index(line, bit).and_then(|index| filter.get_mut(index)) {
        *byte |= 1 << (bit % 8);
    }
}

fn is_set(filter: &[u8], line: usize, bit: u32) -> bool {
    byte_index(line, bit)
        .and_then(|index| filter.get(index))
        .is_some_and(|byte| byte & (1 << (bit % 8)) != 0)
}

fn byte_index(line: usize, bit: u32) -> Option<usize> {
    line.checked_mul(LINE_BYTES)?
        .checked_add(usize::try_from(bit / 8).ok()?)
}

/// The filter's 64-bit hash of a key: eight bytes at a time folded by multiplication, then
/// finished with an avalanche so every input bit affects every output bit. In-memory filters
/// over the same keys use it too, so one hash serves every filter a read consults.
pub fn hash(key: &[u8]) -> u64 {
    const MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut state = u64::try_from(key.len())
        .unwrap_or(u64::MAX)
        .wrapping_mul(MULTIPLIER);
    for chunk in key.chunks(8) {
        let mut word = [0_u8; 8];
        if let Some(prefix) = word.get_mut(..chunk.len()) {
            prefix.copy_from_slice(chunk);
        }
        state = (state ^ u64::from_le_bytes(word)).wrapping_mul(MULTIPLIER);
        state ^= state >> 29;
    }
    state ^= state >> 30;
    state = state.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    state ^= state >> 27;
    state = state.wrapping_mul(0x94D0_49BB_1331_11EB);
    state ^ (state >> 31)
}
