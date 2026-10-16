//! Tables whose data blocks are not all at hand: a walk that pauses on a missing block and
//! resumes with it reads exactly what the table holds, in every direction.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use proptest::prelude::*;

use super::block::Block;
use super::codec_tests::EVERY;
use super::*;
use crate::error::{Error, Result};

fn bytewise(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    left.cmp(right)
}

fn whole(key: &[u8]) -> &[u8] {
    key
}

fn build(model: &BTreeMap<Vec<u8>, Vec<u8>>, compression: Compression) -> Vec<u8> {
    let options = TableOptions {
        block_bytes: 64,
        restart_interval: 4,
        bloom_bits_per_key: 10,
        compression,
    };
    let mut builder = TableBuilder::new(options, bytewise, whole);
    for (key, value) in model {
        builder.add(key, value).unwrap();
    }
    builder.finish().unwrap()
}

/// A source holding only some of a file's data blocks; the others are "not at hand".
#[derive(Debug)]
struct Sparse {
    file: Arc<[u8]>,
    /// Block offsets at hand: a block is added once a walk resumes with it, when `learn` is set.
    present: Mutex<BTreeSet<u64>>,
    learn: bool,
}

impl Sparse {
    fn new(file: &[u8], present: BTreeSet<u64>, learn: bool) -> Arc<Self> {
        Arc::new(Self {
            file: file.into(),
            present: Mutex::new(present),
            learn,
        })
    }

    /// The block at `handle`, read from the file as a reader of storage would.
    fn read(&self, handle: BlockHandle) -> Result<Block> {
        let start = usize::try_from(handle.offset()).unwrap();
        let end = start + usize::try_from(handle.size()).unwrap();
        let block = unseal_block(&self.file[start..end], handle.offset(), None, bytewise)?;
        if self.learn {
            self.present.lock().unwrap().insert(handle.offset());
        }
        Ok(block)
    }
}

impl BlockSource for Sparse {
    fn block(&self, handle: BlockHandle) -> Result<Option<Block>> {
        let present = self.present.lock().unwrap().contains(&handle.offset());
        if present {
            self.read(handle).map(Some)
        } else {
            Ok(None)
        }
    }
}

fn open_sparse(file: &[u8], source: Arc<Sparse>) -> Table {
    let len = u64::try_from(file.len()).unwrap();
    Table::open_with(
        len,
        |offset, buf: &mut [u8]| {
            let start = usize::try_from(offset).unwrap();
            buf.copy_from_slice(&file[start..start + buf.len()]);
            Ok::<(), ()>(())
        },
        source,
        None,
        bytewise,
        whole,
    )
    .unwrap()
}

#[derive(Debug, Clone)]
enum Move {
    First,
    Seek(Vec<u8>),
    Last,
    ForPrev(Vec<u8>),
    Next,
    Prev,
}

fn a_move() -> impl Strategy<Value = Move> {
    let target = prop::collection::vec(any::<u8>(), 0..6);
    prop_oneof![
        1 => Just(Move::First),
        1 => target.clone().prop_map(Move::Seek),
        1 => Just(Move::Last),
        1 => target.prop_map(Move::ForPrev),
        4 => Just(Move::Next),
        4 => Just(Move::Prev),
    ]
}

fn apply(iter: &mut TableIter, step: &Move) {
    match step {
        Move::First => iter.seek_to_first(),
        Move::Seek(target) => iter.seek(target),
        Move::Last => iter.seek_to_last(),
        Move::ForPrev(target) => iter.seek_for_prev(target),
        Move::Next => iter.next(),
        Move::Prev => iter.prev(),
    }
}

/// Where `step` leaves a walk over `entries` that stood at `at`: the model of a table iterator.
/// Off either end a walk stays off until a seek.
fn expected(entries: &[(Vec<u8>, Vec<u8>)], at: Option<usize>, step: &Move) -> Option<usize> {
    let ceiling = |target: &[u8]| entries.partition_point(|(key, _)| key.as_slice() < target);
    let floor = |target: &[u8]| entries.partition_point(|(key, _)| key.as_slice() <= target);
    match step {
        Move::First => (!entries.is_empty()).then_some(0),
        Move::Seek(target) => Some(ceiling(target)).filter(|index| *index < entries.len()),
        Move::Last => entries.len().checked_sub(1),
        Move::ForPrev(target) => floor(target).checked_sub(1),
        Move::Next => at
            .map(|index| index + 1)
            .filter(|index| *index < entries.len()),
        Move::Prev => at.and_then(|index| index.checked_sub(1)),
    }
}

/// Resumes `iter` until no block is missing, at most `limit` times.
fn finish(iter: &mut TableIter, source: &Sparse, limit: usize) -> usize {
    let mut resumed = 0;
    while let Some(handle) = iter.pending() {
        assert!(!iter.valid(), "a pending walk is on an entry");
        assert!(iter.status().is_ok(), "a pending walk reports an error");
        assert!(resumed < limit, "the walk keeps asking for blocks");
        iter.resume(source.read(handle).unwrap());
        resumed += 1;
    }
    resumed
}

proptest! {
    #[test]
    fn a_walk_that_waits_for_blocks_reads_what_the_table_holds(
        model in prop::collection::btree_map(
            prop::collection::vec(any::<u8>(), 1..6),
            prop::collection::vec(any::<u8>(), 0..24),
            0..80,
        ),
        codec in 0..EVERY.len(),
        present in prop::collection::btree_set(0_u64..4096, 0..20),
        learn in any::<bool>(),
        moves in prop::collection::vec(a_move(), 1..40),
    ) {
        let compression = EVERY[codec];
        let file = build(&model, compression);
        let entries: Vec<_> = model.into_iter().collect();
        let source = Sparse::new(&file, present, learn);
        let sparse = open_sparse(&file, Arc::clone(&source));
        prop_assert_eq!(sparse.entries(), u64::try_from(entries.len()).unwrap());
        prop_assert!(entries.iter().all(|(key, _)| sparse.may_contain(key)));
        let mut walked = sparse.iter();
        let mut at = None;
        let blocks = file.len() / 8 + 2;
        for step in &moves {
            at = expected(&entries, at, step);
            apply(&mut walked, step);
            finish(&mut walked, &source, blocks);
            let entry = at.map(|index: usize| (entries[index].0.as_slice(), entries[index].1.as_slice()));
            prop_assert_eq!(walked.valid().then(|| (walked.key(), walked.value())), entry, "after {:?}", step);
            prop_assert!(walked.status().is_ok());
        }
    }
}

/// A table of one entry.
fn one_entry() -> Vec<u8> {
    build(
        &BTreeMap::from([(b"a".to_vec(), b"1".to_vec())]),
        Compression::None,
    )
}

#[test]
fn a_failed_read_while_opening_is_the_reader_s_error() {
    let file = one_entry();
    let source = Sparse::new(&file, BTreeSet::new(), false);
    let opened = Table::open_with(
        u64::try_from(file.len()).unwrap(),
        |_, _: &mut [u8]| Err("disk gone"),
        source,
        None,
        bytewise,
        whole,
    );
    assert!(matches!(opened, Err(OpenError::Read("disk gone"))));
}

#[test]
fn a_handle_past_the_file_is_refused_before_anything_is_read() {
    let file = one_entry();
    let len = u64::try_from(file.len()).unwrap();
    // A footer whose index handle claims an enormous block: refused, never allocated.
    let mut crafted = file.clone();
    let footer = crafted.len() - FOOTER_BYTES;
    crafted[footer + 24..footer + 32].copy_from_slice(&(u64::MAX / 2).to_le_bytes());
    let checksum = crc32c::crc32c(&crafted[footer..footer + 52]);
    crafted[footer + 52..footer + 56].copy_from_slice(&checksum.to_le_bytes());
    let source = Sparse::new(&crafted, BTreeSet::new(), false);
    let opened = Table::open_with(
        len,
        |offset, buf: &mut [u8]| {
            assert!(
                buf.len() <= crafted.len(),
                "asked to read {} bytes",
                buf.len()
            );
            let start = usize::try_from(offset).unwrap();
            buf.copy_from_slice(&crafted[start..start + buf.len()]);
            Ok::<(), ()>(())
        },
        source,
        None,
        bytewise,
        whole,
    );
    assert!(matches!(
        opened,
        Err(OpenError::Format(Error::Malformed { .. }))
    ));
}

#[test]
fn a_damaged_block_does_not_unseal() {
    let file = one_entry();
    // The first data block starts the file; damage its first byte.
    let mut damaged = file;
    damaged[0] ^= 0x41;
    let source = Sparse::new(&damaged, BTreeSet::new(), false);
    let sparse = open_sparse(&damaged, Arc::clone(&source));
    let mut iter = sparse.iter();
    iter.seek_to_first();
    let handle = iter.pending().unwrap();
    assert!(matches!(source.read(handle), Err(Error::Malformed { .. })));
}

#[test]
fn a_seek_abandons_a_move_still_waiting_for_its_block() {
    let model: BTreeMap<_, _> = (0..40_u8)
        .map(|n| (format!("k{n:02}").into_bytes(), vec![n; 9]))
        .collect();
    let file = build(&model, Compression::None);
    let source = Sparse::new(&file, BTreeSet::new(), true);
    let table = open_sparse(&file, Arc::clone(&source));
    let mut iter = table.iter();
    // Bring the last block to hand, then start a move that waits for the first one.
    iter.seek_to_last();
    finish(&mut iter, &source, 4);
    iter.seek_to_first();
    assert!(iter.pending().is_some());
    // The caller gives up on that move and seeks into the block at hand.
    iter.seek(b"k39");
    assert_eq!(iter.pending(), None);
    assert!(iter.valid());
    assert_eq!(iter.key(), b"k39");
}
