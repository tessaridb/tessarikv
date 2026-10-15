//! A position in the list that moves forward and backward.

use super::node::Node;
use super::search::Position;
use super::{KeyComparator, SkipList};

/// A position in a [`SkipList`]: on an entry, or invalid. Weakly consistent (see the module
/// documentation): it never shows an entry out of order or partly written.
pub struct Cursor<'a, C> {
    list: &'a SkipList<C>,
    node: Option<Node<'a>>,
}

impl<C: KeyComparator> SkipList<C> {
    /// A cursor, initially invalid.
    pub fn cursor(&self) -> Cursor<'_, C> {
        Cursor {
            list: self,
            node: None,
        }
    }

    /// The value of the key equal to `key`.
    #[cfg(not(loom))]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let mut cursor = self.cursor();
        cursor.seek(key);
        let node = cursor.node?;
        (self.compare_node(&node, key)? == std::cmp::Ordering::Equal).then(|| cursor.value())
    }

    /// The published node at `reference`, or none for the null link.
    fn published(&self, reference: u64) -> Option<Node<'_>> {
        if reference == 0 {
            None
        } else {
            self.node(reference)
        }
    }
}

impl<'a, C: KeyComparator> Cursor<'a, C> {
    /// Whether the cursor is on an entry.
    pub fn valid(&self) -> bool {
        self.node.is_some()
    }

    /// Moves to the least key not less than `key`.
    pub fn seek(&mut self, key: &[u8]) {
        // The successor the search itself loaded, not a re-read: a re-read could meet a key
        // inserted after the search passed and land before the target.
        self.node = self
            .list
            .last_before(key, false)
            .and_then(|(_, after)| self.list.published(after));
    }

    /// Moves to the least key.
    pub fn seek_to_first(&mut self) {
        self.node = self
            .list
            .published(self.list.next_at(Position::default(), 0).unwrap_or(0));
    }

    /// Moves to the greatest key.
    pub fn seek_to_last(&mut self) {
        let mut position = Position::default();
        for level in (0..self.list.head.len()).rev() {
            while let Some(next) = self
                .list
                .next_at(position, level)
                .and_then(|next| self.list.published(next))
            {
                position = Position::from(&next);
            }
        }
        self.node = position.node;
    }

    /// Moves to the next key; becomes invalid past the last one.
    pub fn next(&mut self) {
        self.node = self
            .node
            .and_then(|node| self.list.next_at(Position::from(&node), 0))
            .and_then(|next| self.list.published(next));
    }

    /// Moves to the previous key; becomes invalid before the first one.
    pub fn prev(&mut self) {
        let Some(node) = self.node else { return };
        let before = self
            .list
            .with_key(&node, |key| self.list.last_before(key, false))
            .flatten();
        self.node = before.and_then(|(position, _)| position.node);
    }

    /// A copy of the current key.
    #[cfg(all(test, loom))]
    pub(crate) fn copy_key(&self) -> Option<Vec<u8>> {
        self.list.with_key(&self.node?, <[u8]>::to_vec)
    }

    /// A copy of the current value.
    #[cfg(all(test, loom))]
    pub(crate) fn copy_value(&self) -> Option<Vec<u8>> {
        self.list.with_value(&self.node?, <[u8]>::to_vec)
    }
}

#[cfg(not(loom))]
impl<'a, C: KeyComparator> Cursor<'a, C> {
    /// The current key; empty when invalid.
    pub fn key(&self) -> &'a [u8] {
        self.bytes(|node| Some((node.key_offset()?, node.key_len)))
    }

    /// The current value; empty when invalid.
    pub fn value(&self) -> &'a [u8] {
        self.bytes(|node| Some((node.value_offset()?, node.value_len)))
    }

    /// The bytes at `(offset in record, len)` of the current node.
    fn bytes(&self, range: impl FnOnce(&Node<'a>) -> Option<(usize, u32)>) -> &'a [u8] {
        self.node
            .and_then(|node| {
                let (relative, len) = range(&node)?;
                node.slice(relative, len)
            })
            .unwrap_or(&[])
    }
}
