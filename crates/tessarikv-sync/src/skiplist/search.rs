//! Walking the list: one level at a time, from the head down.

use std::cmp::Ordering as KeyOrder;

use tessarikv_constants::SKIPLIST_MAX_HEIGHT;

use super::node::Node;
use super::{KeyComparator, SkipList};
use crate::shim::Ordering;

/// A position to walk from: the head (`reference == 0`) or a node.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Position {
    pub(super) reference: u64,
    pub(super) link_index: u64,
}

impl From<&Node> for Position {
    fn from(node: &Node) -> Self {
        Self {
            reference: node.reference,
            link_index: node.link_index,
        }
    }
}

/// Where a key belongs at every level: the last position before it and the link after that.
pub(super) struct Splice {
    pub(super) preds: [Position; SKIPLIST_MAX_HEIGHT],
    pub(super) succs: [u64; SKIPLIST_MAX_HEIGHT],
}

impl Splice {
    pub(super) fn new() -> Self {
        Self {
            preds: [Position::default(); SKIPLIST_MAX_HEIGHT],
            succs: [0; SKIPLIST_MAX_HEIGHT],
        }
    }
}

impl<C: KeyComparator> SkipList<C> {
    /// Compares a published node's key with `key`.
    pub(super) fn compare_node(&self, node: &Node, key: &[u8]) -> Option<KeyOrder> {
        self.with_key(node, |node_key| self.comparator.compare(node_key, key))
    }

    /// The level-0 successor reference of `position` (0 = none).
    pub(super) fn next_at(&self, position: Position, level: usize) -> Option<u64> {
        // Acquire: the loaded node's record and tower were published by its linking CAS.
        Some(
            self.link(position.reference, position.link_index, level)?
                .load(Ordering::Acquire),
        )
    }

    /// From `start`, moves right at `level` while the next key is less than `key` (or, with
    /// `or_equal`, not greater). Returns the last position passed and the link after it.
    pub(super) fn walk(
        &self,
        level: usize,
        start: Position,
        key: &[u8],
        or_equal: bool,
    ) -> Option<(Position, u64)> {
        let mut position = start;
        loop {
            let next = self.next_at(position, level)?;
            if next == 0 {
                return Some((position, 0));
            }
            let node = self.node(next)?;
            let order = self.compare_node(&node, key)?;
            if order == KeyOrder::Less || (or_equal && order == KeyOrder::Equal) {
                position = Position::from(&node);
            } else {
                return Some((position, next));
            }
        }
    }

    /// Where `key` belongs at levels `0..levels` (searched from the top of the list or of
    /// `levels`, whichever is higher, since a taller list must be walked from its top).
    pub(super) fn splice(&self, key: &[u8], levels: usize, splice: &mut Splice) -> Option<()> {
        // Relaxed: only chooses the starting level.
        let current = usize::try_from(self.max_height.load(Ordering::Relaxed)).ok()?;
        let top = current.max(levels).clamp(1, SKIPLIST_MAX_HEIGHT);
        let mut position = Position::default();
        for level in (0..top).rev() {
            let (pred, succ) = self.walk(level, position, key, false)?;
            if let (Some(slot_pred), Some(slot_succ)) =
                (splice.preds.get_mut(level), splice.succs.get_mut(level))
            {
                *slot_pred = pred;
                *slot_succ = succ;
            }
            position = pred;
        }
        Some(())
    }

    /// The last position whose key is less than `key` (or, with `or_equal`, not greater), and
    /// the level-0 link the search saw after it: the successor as of the linearization point.
    pub(super) fn last_before(&self, key: &[u8], or_equal: bool) -> Option<(Position, u64)> {
        let current = usize::try_from(self.max_height.load(Ordering::Relaxed)).ok()?;
        let mut position = Position::default();
        let mut after = 0;
        for level in (0..current.clamp(1, SKIPLIST_MAX_HEIGHT)).rev() {
            (position, after) = self.walk(level, position, key, or_equal)?;
        }
        Some((position, after))
    }
}
