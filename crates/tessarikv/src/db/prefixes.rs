//! Key prefixes as conflict buckets: what a write records so that a serializable transaction can
//! learn whether anything was written into a range it read, without remembering the keys.
//!
//! A key `k` stands for the buckets `B(k)`: "a key starting with `k[..d]`" for every depth `d`
//! from 1 up to `min(DEPTH, |k|)`, and "exactly `k`" when `k` is shorter than [`DEPTH`]. A range
//! `[start, end)` stands for its cover: the buckets of a walk down the tree of prefixes that
//! names every subtree lying wholly inside the range, descends into the at most two subtrees per
//! depth that straddle a bound, and stops at [`DEPTH`], naming the straddling subtree there whole
//! (keys sharing that prefix with a bound may then conflict without being in the range). Where
//! more than [`FANOUT`] children of a straddling subtree lie wholly inside, the walk names that
//! subtree instead, so a cover stays near `2 · FANOUT` buckets per depth. Every key in the range
//! shares a bucket with the cover, so a write recorded under `B(k)` is seen by a check of the
//! cover; keys outside it share one only through a straddling subtree. There is no bucket for
//! the empty prefix: every write would record it.

/// How many leading bytes a bucket distinguishes.
pub(super) const DEPTH: usize = 8;

/// The most wholly-inside children a cover names one by one before naming their parent.
const FANOUT: usize = 16;

/// The kinds of bucket, kept apart in the hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// Every key starting with the bytes.
    Prefix,
    /// Exactly the bytes, a key shorter than the depth.
    Exact,
}

/// Calls `emit` with the buckets of `key`.
pub(super) fn buckets_of(key: &[u8], emit: impl FnMut(Kind, &[u8])) {
    buckets_at(DEPTH, key, emit);
}

/// Calls `emit` with the cover of `[start, end)` (unbounded above when `end` is `None`).
pub(super) fn cover(start: &[u8], end: Option<&[u8]>, mut emit: impl FnMut(Kind, &[u8])) {
    let mut path = Vec::with_capacity(DEPTH);
    cover_at(Shape::FULL, &mut path, start, end, &mut emit);
}

fn buckets_at(depth: usize, key: &[u8], mut emit: impl FnMut(Kind, &[u8])) {
    for length in 1..=depth.min(key.len()) {
        if let Some(prefix) = key.get(..length) {
            emit(Kind::Prefix, prefix);
        }
    }
    if key.len() < depth {
        emit(Kind::Exact, key);
    }
}

/// Where the keys starting with a prefix lie relative to a range.
enum Relation {
    Inside,
    Outside,
    Straddling,
}

fn relation(prefix: &[u8], start: &[u8], end: Option<&[u8]>) -> Relation {
    // Every key with the prefix is at least the prefix; below `start` only if the prefix is
    // below it and not a prefix of it.
    let below = prefix < start && !start.starts_with(prefix);
    let above = end.is_some_and(|end| end <= prefix);
    if below || above {
        return Relation::Outside;
    }
    let from_start = start <= prefix;
    let before_end = end.is_none_or(|end| prefix < end && !end.starts_with(prefix));
    if from_start && before_end {
        Relation::Inside
    } else {
        Relation::Straddling
    }
}

/// How deep a cover walks and how many inside children it names before naming their parent.
#[derive(Debug, Clone, Copy)]
struct Shape {
    depth: usize,
    fanout: usize,
}

impl Shape {
    const FULL: Self = Self {
        depth: DEPTH,
        fanout: FANOUT,
    };
}

/// Emits the cover of the range within the subtree of `path`, which straddles a bound (or is
/// the root).
fn cover_at(
    shape: Shape,
    path: &mut Vec<u8>,
    start: &[u8],
    end: Option<&[u8]>,
    emit: &mut impl FnMut(Kind, &[u8]),
) {
    if path.len() >= shape.depth {
        emit(Kind::Prefix, path);
        return;
    }
    if !path.is_empty() && inside_children(path, start, end) > shape.fanout {
        // The subtree's prefix bucket holds every key under it, including the path itself.
        emit(Kind::Prefix, path);
        return;
    }
    if start <= path.as_slice() && end.is_none_or(|end| path.as_slice() < end) {
        emit(Kind::Exact, path);
    }
    for byte in u8::MIN..=u8::MAX {
        path.push(byte);
        match relation(path, start, end) {
            Relation::Inside => emit(Kind::Prefix, path),
            Relation::Outside => {}
            Relation::Straddling => cover_at(shape, path, start, end, emit),
        }
        path.pop();
    }
}

/// How many children of `path` lie wholly inside the range.
fn inside_children(path: &mut Vec<u8>, start: &[u8], end: Option<&[u8]>) -> usize {
    let mut inside = 0_usize;
    for byte in u8::MIN..=u8::MAX {
        path.push(byte);
        if matches!(relation(path, start, end), Relation::Inside) {
            inside = inside.saturating_add(1);
        }
        path.pop();
    }
    inside
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{Kind, Relation, Shape, buckets_at, cover_at, relation};

    type Bucket = (Kind, Vec<u8>);

    fn cover_set(shape: Shape, start: &[u8], end: Option<&[u8]>) -> BTreeSet<(u8, Vec<u8>)> {
        let mut set = BTreeSet::new();
        cover_at(shape, &mut Vec::new(), start, end, &mut |kind, bytes| {
            set.insert((tag(kind), bytes.to_vec()));
        });
        set
    }

    fn buckets(depth: usize, key: &[u8]) -> Vec<Bucket> {
        let mut all = Vec::new();
        buckets_at(depth, key, |kind, bytes| all.push((kind, bytes.to_vec())));
        all
    }

    fn tag(kind: Kind) -> u8 {
        match kind {
            Kind::Prefix => 0,
            Kind::Exact => 1,
        }
    }

    /// Every key over a three-letter alphabet up to `length` bytes.
    fn keys(length: usize) -> Vec<Vec<u8>> {
        let mut all = vec![Vec::new()];
        let mut frontier = vec![Vec::new()];
        for _ in 0..length {
            frontier = frontier
                .iter()
                .flat_map(|key: &Vec<u8>| {
                    (0_u8..3).map(move |byte| [key.as_slice(), &[byte]].concat())
                })
                .collect();
            all.extend(frontier.iter().cloned());
        }
        all
    }

    /// Against brute force at depths 1 to 3, naming children one by one or folding them into
    /// their parent: every key in the range shares a bucket with the cover, and a key outside it
    /// shares one only through the prefix of a subtree that straddles a bound.
    #[test]
    fn the_cover_meets_every_key_in_the_range_and_only_cut_subtrees_outside_it() {
        for (depth, fanout) in (1..=3).flat_map(|depth| [(depth, 256), (depth, 1)]) {
            let shape = Shape { depth, fanout };
            let keys = keys(depth + 1);
            let ends: Vec<Option<&[u8]>> = keys
                .iter()
                .map(|key| Some(key.as_slice()))
                .chain([None])
                .collect();
            for start in &keys {
                for &end in &ends {
                    let set = cover_set(shape, start, end);
                    for key in &keys {
                        let inside = start <= key && end.is_none_or(|end| key.as_slice() < end);
                        let shared: Vec<_> = buckets(depth, key)
                            .into_iter()
                            .filter(|(kind, bytes)| set.contains(&(tag(*kind), bytes.clone())))
                            .collect();
                        if inside {
                            assert!(
                                !shared.is_empty(),
                                "{key:?} in [{start:?}, {end:?}) at {depth}"
                            );
                        } else {
                            assert!(
                                shared.iter().all(|(kind, bytes)| *kind == Kind::Prefix
                                    && matches!(relation(bytes, start, end), Relation::Straddling)),
                                "{key:?} outside [{start:?}, {end:?}) at {depth}: {shared:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// At most two subtrees straddle a bound at each depth and each names at most `FANOUT`
    /// children, so a check costs a bounded number of loads whatever the range (the root, which
    /// has no bucket of its own, may name all of its children).
    #[test]
    fn a_cover_is_bounded_by_two_straddling_subtrees_per_depth() {
        let bound = 256 + 2 * (super::FANOUT + 2) * super::DEPTH;
        for (start, end) in [
            (&b"user:1000"[..], Some(&b"user:1999"[..])),
            (b"", None),
            (b"a", Some(b"b")),
            (
                b"\x00\xff\x00\xff\x00\xff\x00\xff\x00",
                Some(b"\xff\x00\xff\x00\xff\x00\xff\x00"),
            ),
        ] {
            let set = cover_set(Shape::FULL, start, end);
            assert!(
                !set.is_empty() && set.len() <= bound,
                "{start:?}: {}",
                set.len()
            );
        }
    }
}
