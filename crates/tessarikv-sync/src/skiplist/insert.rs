//! Inserting: link at level 0 (the linearization point), then upward.

use std::cmp::Ordering as KeyOrder;

use tessarikv_constants::SKIPLIST_MAX_HEIGHT;

use super::height::random_height;
use super::search::{Position, Splice};
use super::{Insert, KeyComparator, SkipList};
use crate::error::ArenaError;
use crate::shim::Ordering;

impl<C: KeyComparator> SkipList<C> {
    /// Adds `key ↦ value` unless an equal key is present.
    ///
    /// # Errors
    ///
    /// [`ArenaError`] when the record cannot be allocated: a key or value longer than
    /// `u32::MAX` bytes, or an exhausted arena.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<Insert, ArenaError> {
        self.insert_with_height(key, value, random_height())
    }

    /// [`SkipList::insert`] with a chosen tower height (clamped to `1..=SKIPLIST_MAX_HEIGHT`), so
    /// tests and models can build a given shape.
    pub(crate) fn insert_with_height(
        &self,
        key: &[u8],
        value: &[u8],
        height: usize,
    ) -> Result<Insert, ArenaError> {
        let height = height.clamp(1, SKIPLIST_MAX_HEIGHT);
        let mut splice = Splice::new();
        if self.locate(key, height, &mut splice)? {
            return Ok(Insert::AlreadyPresent);
        }
        let node = self.write_node(key, value, height)?;
        let me = Position::from(&node);
        loop {
            for level in 0..height {
                let succ = splice.succs.get(level).copied().unwrap_or(0);
                // Relaxed: the node is unpublished; the linking CAS publishes it.
                self.link(me.reference, me.link_index, level)
                    .ok_or(ArenaError::Internal)?
                    .store(succ, Ordering::Relaxed);
            }
            let pred = splice.preds.first().copied().unwrap_or_default();
            let expected = splice.succs.first().copied().unwrap_or(0);
            let slot = self
                .link(pred.reference, pred.link_index, 0)
                .ok_or(ArenaError::Internal)?;
            // Success Release: publishes the record and tower; this is the linearization point.
            // Failure Relaxed: the value is discarded; the re-search loads with Acquire.
            if slot
                .compare_exchange(
                    expected,
                    node.reference,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
            if self.locate(key, height, &mut splice)? {
                // Another insert of an equal key won; this node stays unused in the arena.
                return Ok(Insert::AlreadyPresent);
            }
        }
        // Relaxed: only where searches start.
        self.max_height
            .fetch_max(u64::try_from(height).unwrap_or(1), Ordering::Relaxed);
        for level in 1..height {
            self.link_upper(key, me, level, &mut splice)?;
        }
        Ok(Insert::Inserted)
    }

    /// Fills `splice` for `key`; `true` when an equal key is already linked at level 0.
    fn locate(&self, key: &[u8], height: usize, splice: &mut Splice) -> Result<bool, ArenaError> {
        self.splice(key, height, splice)
            .ok_or(ArenaError::Internal)?;
        let succ = splice.succs.first().copied().unwrap_or(0);
        if succ == 0 {
            return Ok(false);
        }
        let node = self.node(succ).ok_or(ArenaError::Internal)?;
        Ok(self.compare_node(&node, key).ok_or(ArenaError::Internal)? == KeyOrder::Equal)
    }

    /// Links the published node `me` at `level`, re-walking from the old predecessor on conflict.
    fn link_upper(
        &self,
        key: &[u8],
        me: Position,
        level: usize,
        splice: &mut Splice,
    ) -> Result<(), ArenaError> {
        let (mut pred, mut succ) = match (splice.preds.get(level), splice.succs.get(level)) {
            (Some(&pred), Some(&succ)) => (pred, succ),
            _ => return Err(ArenaError::Exhausted),
        };
        loop {
            let slot = self
                .link(pred.reference, pred.link_index, level)
                .ok_or(ArenaError::Internal)?;
            // Success Release: publishes this level of the tower. Failure Relaxed: the value
            // is discarded and `walk` reloads with Acquire.
            if slot
                .compare_exchange(succ, me.reference, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
            (pred, succ) = self
                .walk(level, pred, key, false)
                .ok_or(ArenaError::Internal)?;
            // Relaxed: not yet reachable at this level; the next CAS publishes it.
            self.link(me.reference, me.link_index, level)
                .ok_or(ArenaError::Internal)?
                .store(succ, Ordering::Relaxed);
        }
    }
}
