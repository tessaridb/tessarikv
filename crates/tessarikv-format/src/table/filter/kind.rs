//! Which filter a table keeps over its keys, and the filter read back from its bytes.

#[cfg(test)]
mod tests;

use super::ribbon::{self, RibbonFilter};
use super::{BloomBuilder, BloomFilter, Probing, TRAILER_BYTES, hash};
use crate::error::Result;

/// The kind of whole-key filter a table writes. Both answer "certainly absent" or "possibly
/// present" and never miss a key the table holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FilterPolicy {
    /// A bloom filter blocked by cache line: about one absent key in a hundred passes at ten bits
    /// per key, and a lookup touches one 64-byte line.
    #[default]
    Bloom,
    /// A ribbon filter: at the same bits-per-key setting it takes about three quarters of a bloom
    /// filter's bytes and passes somewhat fewer absent keys (about one in 130 at ten), and costs
    /// more to build. Falls back to a bloom filter for a table whose keys no seed solves.
    Ribbon,
}

/// Collects the hashes of a table's keys, then lays out the filter `policy` names.
#[derive(Debug)]
pub(crate) struct FilterBuilder {
    policy: FilterPolicy,
    bloom: BloomBuilder,
}

impl FilterBuilder {
    /// A builder spending about `bits_per_key` bits per key.
    pub(crate) fn new(policy: FilterPolicy, bits_per_key: u32) -> Self {
        Self {
            policy,
            bloom: BloomBuilder::new(bits_per_key),
        }
    }

    /// Records `key`.
    pub(crate) fn add(&mut self, key: &[u8]) {
        self.bloom.add(key);
    }

    /// The filter of every key added; the builder is empty again afterwards.
    pub(crate) fn finish(&mut self) -> Vec<u8> {
        if self.policy == FilterPolicy::Ribbon
            && let Some(bytes) = ribbon::build(&self.bloom.hashes, self.bloom.bits_per_key)
        {
            self.bloom.hashes.clear();
            return bytes;
        }
        self.bloom.finish()
    }
}

/// A table's decoded whole-key filter.
#[derive(Debug, Clone)]
pub(crate) enum TableFilter {
    Bloom(BloomFilter),
    Ribbon(RibbonFilter),
}

impl TableFilter {
    /// Decodes `bytes`, a bloom filter whose bits were picked by `probing` or a ribbon filter,
    /// told apart by the byte where a bloom filter keeps its probe count.
    pub(crate) fn decode(bytes: Vec<u8>, probing: Probing) -> Result<Self> {
        let marker = bytes
            .len()
            .checked_sub(TRAILER_BYTES)
            .and_then(|at| bytes.get(at));
        if marker == Some(&ribbon::MARKER) {
            return RibbonFilter::new(&bytes).map(Self::Ribbon);
        }
        BloomFilter::with_probing(bytes, probing).map(Self::Bloom)
    }

    /// Whether `key` may be in the table: `false` means it certainly is not.
    pub(crate) fn may_contain(&self, key: &[u8]) -> bool {
        match self {
            Self::Bloom(filter) => filter.may_contain(key),
            Self::Ribbon(filter) => filter.may_contain(hash(key)),
        }
    }
}
