//! A ribbon filter: a key passes when a few bits it selects from a solved linear system
//! reproduce its fingerprint.
//!
//! ```text
//! [solution word: u64]*  [seed: u32]  [result bits: u8]  [marker: u8]  [block count: u32]
//! ```
//!
//! Each key gives one equation over the bits of `64 × block count` slots: a 64-bit coefficient
//! starting at a slot of its own and an `r`-bit fingerprint, where `r` is the result bits. The
//! builder solves the equations of every key, eliminating each as it arrives against the rows
//! already held, so a slot keeps at most one row whose lowest coefficient bit is that slot; then it
//! substitutes back from the last slot. A key passes when the slots its coefficient selects XOR to
//! its fingerprint, which every added key's do: an absent key passes with probability 2^-r.
//!
//! The solution is stored in 64-slot blocks, `r` words each, word `k` holding bit `k` of every
//! slot of the block, so a lookup reads two words per result bit. The marker sits where a bloom
//! filter keeps its probe count (1 to 30), which tells the two apart.
//!
//! The way a key's equation is derived from its hash is part of the format.

#[cfg(test)]
mod tests;

use crate::error::{Error, Result};

/// Bytes after the solution: seed, result bits, marker and block count.
pub(super) const TRAILER_BYTES: usize = 10;

/// The byte five from the end of a ribbon filter, where a bloom filter keeps its probe count.
pub(super) const MARKER: u8 = 0x80;

/// Slots of one block: the width of a coefficient.
const BLOCK_SLOTS: u64 = 64;

/// The most result bits a filter holds.
const MAX_RESULT_BITS: u8 = 32;

/// Seeds tried before the builder gives up.
const MAX_SEEDS: u32 = 8;

/// Spreads a seed over every bit of a key's hash.
const SEED_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

/// Separates a key's coefficient from its start.
const COEFFICIENT_SALT: u64 = 0xD6E8_FEB8_6659_FD93;

/// Separates a key's fingerprint from its start and coefficient.
const FINGERPRINT_MIX: u64 = 0xFF51_AFD7_ED55_8CCD;

/// One key's equation.
#[derive(Debug, PartialEq, Eq)]
struct Row {
    /// The slot of the coefficient's lowest bit, at most `slots - 64`.
    start: u64,
    /// Which of the 64 slots from `start` the key selects; bit 0 is always set.
    coefficient: u64,
    /// What the selected slots must XOR to.
    fingerprint: u32,
}

/// A 64-bit finaliser: every input bit affects every output bit.
fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// The equation of the key whose filter hash is `key_hash`, over `slots` slots (a multiple of
/// 64, at least 64) with `result_bits` bits of fingerprint.
fn row(key_hash: u64, seed: u32, slots: u64, result_bits: u8) -> Row {
    let mixed = mix(key_hash ^ u64::from(seed).wrapping_mul(SEED_MIX));
    let starts = u128::from(slots.saturating_sub(BLOCK_SLOTS - 1));
    let scaled = u128::from(mixed >> 32).wrapping_mul(starts) >> 32;
    let fingerprint = mixed
        .wrapping_mul(FINGERPRINT_MIX)
        .checked_shr(64_u32.saturating_sub(u32::from(result_bits)))
        .unwrap_or(0);
    Row {
        start: u64::try_from(scaled).unwrap_or(0),
        coefficient: mix(mixed ^ COEFFICIENT_SALT) | 1,
        fingerprint: u32::try_from(fingerprint).unwrap_or(0),
    }
}

/// Result bits for `bits_per_key`: about seven tenths of them, since the solution's slots
/// outnumber the keys by a few hundredths and the fingerprint needs no more to match a bloom
/// filter of the full width.
pub(super) fn result_bits(bits_per_key: u32) -> u8 {
    let bits = (bits_per_key.saturating_mul(7) / 10).clamp(1, u32::from(MAX_RESULT_BITS));
    u8::try_from(bits).unwrap_or(MAX_RESULT_BITS)
}

/// The ribbon filter of keys whose filter hashes are `hashes`, or `None` when there are none or
/// no seed solves them.
pub(super) fn build(hashes: &[u64], bits_per_key: u32) -> Option<Vec<u8>> {
    let keys = u64::try_from(hashes.len()).ok().filter(|&keys| keys > 0)?;
    let result_bits = result_bits(bits_per_key);
    let mut wanted = keys.saturating_add(keys / 12).saturating_add(BLOCK_SLOTS);
    for seed in 0..MAX_SEEDS {
        if seed > 0 && seed % 2 == 0 {
            wanted = wanted.saturating_add(keys / 32).saturating_add(1);
        }
        let blocks = u32::try_from(wanted.div_ceil(BLOCK_SLOTS)).ok()?;
        let slots = u64::from(blocks).saturating_mul(BLOCK_SLOTS);
        if let Some(solution) = solve(hashes, seed, slots, result_bits) {
            return Some(encode(&solution, seed, result_bits, blocks));
        }
    }
    None
}

/// The value of every slot, or `None` when two keys' equations contradict each other.
fn solve(hashes: &[u64], seed: u32, slots: u64, result_bits: u8) -> Option<Vec<u32>> {
    let len = usize::try_from(slots).ok()?;
    let mut coefficients = vec![0_u64; len];
    let mut results = vec![0_u32; len];
    for &key_hash in hashes {
        let Row {
            start,
            mut coefficient,
            mut fingerprint,
        } = row(key_hash, seed, slots, result_bits);
        let mut slot = usize::try_from(start).ok()?;
        // Each row held reaches no further than the rows it came from, so `slot` stays in range.
        loop {
            let held = *coefficients.get(slot)?;
            if held == 0 {
                *coefficients.get_mut(slot)? = coefficient;
                *results.get_mut(slot)? = fingerprint;
                break;
            }
            coefficient ^= held;
            fingerprint ^= *results.get(slot)?;
            if coefficient == 0 {
                // The same equation again holds already; a different fingerprint cannot.
                if fingerprint == 0 {
                    break;
                }
                return None;
            }
            let shift = coefficient.trailing_zeros();
            slot = slot.checked_add(usize::try_from(shift).ok()?)?;
            coefficient = coefficient.wrapping_shr(shift);
        }
    }
    let mut solution = vec![0_u32; len];
    for slot in (0..len).rev() {
        let coefficient = *coefficients.get(slot)?;
        if coefficient == 0 {
            continue;
        }
        let mut value = *results.get(slot)?;
        let mut rest = coefficient.wrapping_shr(1);
        while rest != 0 {
            let offset = usize::try_from(rest.trailing_zeros()).ok()?;
            value ^= *solution.get(slot.checked_add(offset)?.checked_add(1)?)?;
            rest &= rest.wrapping_sub(1);
        }
        *solution.get_mut(slot)? = value;
    }
    Some(solution)
}

/// The filter's bytes: the solution by block and result bit, then the trailer.
fn encode(solution: &[u32], seed: u32, result_bits: u8, blocks: u32) -> Vec<u8> {
    let bits = usize::from(result_bits);
    let block_slots = usize::try_from(BLOCK_SLOTS).unwrap_or(usize::MAX);
    let mut words = vec![0_u64; solution.len().div_ceil(block_slots).saturating_mul(bits)];
    for (block, values) in solution.chunks(block_slots).enumerate() {
        let first = block.saturating_mul(bits);
        for (bit, value) in (0_u32..).zip(values) {
            for result in 0..result_bits {
                if value.wrapping_shr(u32::from(result)) & 1 == 1
                    && let Some(word) = words.get_mut(first.saturating_add(usize::from(result)))
                {
                    *word |= 1_u64.wrapping_shl(bit);
                }
            }
        }
    }
    let mut bytes = Vec::with_capacity(words.len().saturating_mul(8).saturating_add(TRAILER_BYTES));
    for word in words {
        bytes.extend_from_slice(&word.to_le_bytes());
    }
    bytes.extend_from_slice(&seed.to_le_bytes());
    bytes.push(result_bits);
    bytes.push(MARKER);
    bytes.extend_from_slice(&blocks.to_le_bytes());
    bytes
}

/// A decoded ribbon filter.
#[derive(Debug, Clone)]
pub struct RibbonFilter {
    words: Vec<u64>,
    seed: u32,
    result_bits: u8,
    slots: u64,
}

impl RibbonFilter {
    /// Decodes `bytes` as a ribbon filter.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] for fewer bytes than a trailer; [`Error::Malformed`] when the marker
    /// is missing, the result bits are out of range or the block count does not match the bytes.
    pub(super) fn new(bytes: &[u8]) -> Result<Self> {
        let malformed = |reason| Error::Malformed {
            what: "filter",
            reason,
        };
        let Some(words_end) = bytes.len().checked_sub(TRAILER_BYTES) else {
            return Err(Error::Truncated {
                what: "filter",
                needed: 10,
                available: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            });
        };
        let trailer = bytes.get(words_end..).unwrap_or_default();
        let u32_at = |at: usize| {
            trailer
                .get(at..at.saturating_add(4))
                .and_then(|field| field.try_into().ok())
                .map(u32::from_le_bytes)
                .ok_or(malformed("trailer"))
        };
        if trailer.get(5) != Some(&MARKER) {
            return Err(malformed("not a ribbon filter"));
        }
        let result_bits = trailer.get(4).copied().unwrap_or(0);
        if result_bits == 0 || result_bits > MAX_RESULT_BITS {
            return Err(malformed("result bits out of range"));
        }
        let blocks = u32_at(6)?;
        let expected = usize::try_from(blocks)
            .ok()
            .and_then(|blocks| blocks.checked_mul(usize::from(result_bits)))
            .and_then(|words| words.checked_mul(8));
        if blocks == 0 || expected != Some(words_end) {
            return Err(malformed("block count does not match the filter's size"));
        }
        let words = bytes
            .get(..words_end)
            .unwrap_or_default()
            .as_chunks::<8>()
            .0
            .iter()
            .map(|word| u64::from_le_bytes(*word))
            .collect();
        Ok(Self {
            words,
            seed: u32_at(0)?,
            result_bits,
            slots: u64::from(blocks).saturating_mul(BLOCK_SLOTS),
        })
    }

    #[cfg(test)]
    fn blocks_bytes(&self) -> usize {
        self.words.len().saturating_mul(8)
    }

    /// Whether the key whose filter hash is `key_hash` may be present: `false` means it
    /// certainly is not.
    pub(super) fn may_contain(&self, key_hash: u64) -> bool {
        let row = row(key_hash, self.seed, self.slots, self.result_bits);
        let bits = usize::from(self.result_bits);
        let first = usize::try_from(row.start / BLOCK_SLOTS)
            .unwrap_or(usize::MAX)
            .saturating_mul(bits);
        let offset = u32::try_from(row.start % BLOCK_SLOTS).unwrap_or(0);
        let word = |at: usize| self.words.get(at).copied().unwrap_or(0);
        let mut found = 0_u32;
        for result in 0..self.result_bits {
            let at = first.saturating_add(usize::from(result));
            let low = word(at);
            let selected = if offset == 0 {
                low
            } else {
                let high = word(at.saturating_add(bits));
                low.wrapping_shr(offset) | high.wrapping_shl(64_u32.saturating_sub(offset))
            };
            found |=
                ((selected & row.coefficient).count_ones() & 1).wrapping_shl(u32::from(result));
        }
        found == row.fingerprint
    }
}
