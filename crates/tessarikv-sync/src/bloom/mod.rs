//! A bloom filter many threads add to while others read it: no false negatives, a few false
//! positives, and neither side ever waits.
//!
//! Sequential specification: `insert(h)` adds the hash `h` to a set; `may_contain(h)` returns
//! `true` for every hash in the set and, rarely, for one that is not.
//!
//! | Operation | Linearization point | Progress |
//! |---|---|---|
//! | `insert` | its last `fetch_or` | wait-free: at most eight read-modify-writes |
//! | `may_contain` | its last load | wait-free: at most eight loads |
//!
//! A check concurrent with an insert of the same hash may miss it; once the insert has
//! completed, every check that happens after it finds the hash.
//!
//! Layout: lines of 512 bits (one cache line), a hash choosing one line with its high half and
//! six bits in it from successive multiplicative remixes of its low half, so a check touches
//! one cache line. At 26 bits per key about one absent hash in 8 000 passes. Bits are only ever
//! set, and the words live as long as the filter, so nothing is reclaimed.
//!
//! Ordering: every access is Relaxed. The filter publishes nothing by itself; a caller that needs
//! "a reader that sees my write sees its hash" sets the bits before its own Release publication,
//! and the reader checks after the Acquire load that reads it (the message-passing shape).

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

use crate::shim::{AtomicU64, Ordering};

/// Bits per line: one 64-byte cache line.
const LINE_BITS: u32 = 512;

/// Words per line.
const LINE_WORDS: usize = 8;

/// Bits each hash sets in its line.
const PROBES: u32 = 6;

/// Shift keeping the top nine bits of a remix: a bit of the 512-bit line.
const PROBE_SHIFT: u32 = 23;

/// An odd multiplier near 2^32 / phi: each multiplication spreads the low bits into the top ones.
const PROBE_MIX: u32 = 0x9e37_79b9;

/// See the module documentation.
pub struct ConcurrentBloom {
    words: Box<[AtomicU64]>,
    lines: u64,
}

impl std::fmt::Debug for ConcurrentBloom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentBloom")
            .field("lines", &self.lines)
            .finish_non_exhaustive()
    }
}

impl ConcurrentBloom {
    /// A filter of about `bits` bits, rounded down to whole lines and at least one line.
    pub fn new(bits: usize) -> Self {
        let lines = (bits / LINE_WORDS / 64).max(1);
        let words = (0..lines.saturating_mul(LINE_WORDS))
            .map(|_| AtomicU64::new(0))
            .collect();
        Self {
            words,
            lines: u64::try_from(lines).unwrap_or(u64::MAX),
        }
    }

    /// The filter's size in bits: whole lines, so at most what [`ConcurrentBloom::new`] asked for
    /// unless that was less than one line.
    pub fn bits(&self) -> usize {
        usize::try_from(self.lines)
            .unwrap_or(usize::MAX)
            .saturating_mul(usize::try_from(LINE_BITS).unwrap_or(usize::MAX))
    }

    /// Adds `hash`.
    pub fn insert(&self, hash: u64) {
        let (first, masks) = self.probe(hash);
        for (word, mask) in self.words.iter().skip(first).zip(masks) {
            if mask != 0 {
                // Relaxed: see the module documentation; the caller's publication orders it.
                word.fetch_or(mask, Ordering::Relaxed);
            }
        }
    }

    /// Whether `hash` may have been added: `false` means it certainly was not.
    pub fn may_contain(&self, hash: u64) -> bool {
        let (first, masks) = self.probe(hash);
        self.words
            .iter()
            .skip(first)
            .zip(masks)
            // Relaxed: see the module documentation.
            .all(|(word, mask)| mask == 0 || word.load(Ordering::Relaxed) & mask == mask)
    }

    /// The first word of `hash`'s line, and the bits it sets in each word of that line.
    fn probe(&self, hash: u64) -> (usize, [u64; LINE_WORDS]) {
        // The high half, scaled to the line count, picks the line.
        let line = (hash >> 32).wrapping_mul(self.lines) >> 32;
        let first = usize::try_from(line)
            .unwrap_or(0)
            .saturating_mul(LINE_WORDS);
        // Each probe remixes the low half and takes its top nine bits: independent positions
        // in the line, where an arithmetic progression of them would correlate.
        let mut mixed = u32::try_from(hash & 0xFFFF_FFFF).unwrap_or(0);
        let mut masks = [0_u64; LINE_WORDS];
        for _ in 0..PROBES {
            mixed = mixed.wrapping_mul(PROBE_MIX);
            let bit = mixed >> PROBE_SHIFT;
            if let Some(mask) = masks.get_mut(usize::try_from(bit / 64).unwrap_or(0)) {
                *mask |= 1 << (bit % 64);
            }
        }
        (first, masks)
    }
}
