//! Tower heights: geometric with p = 1/4, from a per-thread generator (a shared one would be the
//! hottest contended line in the list).

use std::cell::Cell;
use std::hash::{BuildHasher, RandomState};

use tessarikv_constants::{SKIPLIST_BRANCHING, SKIPLIST_MAX_HEIGHT};

thread_local! {
    /// xorshift64 state; never zero.
    static STATE: Cell<u64> = Cell::new(RandomState::new().hash_one(std::thread::current().id()) | 1);
}

/// A tower height in `1..=SKIPLIST_MAX_HEIGHT`, each level kept with probability 1/branching.
pub(super) fn random_height() -> usize {
    let bits = STATE.with(|state| {
        let mut x = state.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x
    });
    // Each extra level needs log2(branching) more trailing zero bits: probability 1/branching.
    let bits_per_level = SKIPLIST_BRANCHING.ilog2().max(1);
    let levels = bits
        .trailing_zeros()
        .checked_div(bits_per_level)
        .unwrap_or(0);
    let extra = usize::try_from(levels).unwrap_or(SKIPLIST_MAX_HEIGHT);
    extra.saturating_add(1).min(SKIPLIST_MAX_HEIGHT)
}
