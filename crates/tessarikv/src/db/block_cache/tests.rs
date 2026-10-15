use std::sync::Arc;

use tessarikv_format::{Block, BlockBuilder};

use super::{BlockCache, BlockKey};

/// A block of about `bytes` bytes whose one entry names `key`, so a read can tell which block
/// it got.
fn block(key: BlockKey, bytes: usize) -> Block {
    let mut builder = BlockBuilder::new(1, |left, right| left.cmp(right));
    let name = [key.file.to_be_bytes(), key.offset.to_be_bytes()].concat();
    builder
        .add(&name, &vec![0; bytes.saturating_sub(40)])
        .unwrap();
    Block::new(builder.finish(), |left, right| left.cmp(right)).unwrap()
}

/// The key a block from [`block`] names.
fn named(block: &Block) -> BlockKey {
    let mut iter = block.iter();
    iter.seek_to_first();
    let name = iter.key();
    BlockKey {
        file: u64::from_be_bytes(name[..8].try_into().unwrap()),
        offset: u64::from_be_bytes(name[8..16].try_into().unwrap()),
    }
}

fn key(file: u64, offset: u64) -> BlockKey {
    BlockKey { file, offset }
}

#[test]
fn a_cache_of_no_bytes_keeps_nothing() {
    let cache = BlockCache::new(0);
    cache.insert(key(1, 0), block(key(1, 0), 100));
    assert!(cache.get(key(1, 0)).is_none());
    assert_eq!(cache.used_bytes(), 0);
}

#[test]
fn a_block_is_found_under_its_own_key_only() {
    let cache = BlockCache::new(1 << 20);
    cache.insert(key(1, 0), block(key(1, 0), 100));
    cache.insert(key(2, 0), block(key(2, 0), 100));
    assert_eq!(named(&cache.get(key(1, 0)).unwrap()), key(1, 0));
    assert_eq!(named(&cache.get(key(2, 0)).unwrap()), key(2, 0));
    assert!(cache.get(key(1, 64)).is_none());
}

#[test]
fn the_cache_never_holds_more_than_its_capacity_and_drops_the_oldest_first() {
    let capacity = 8 * 1024;
    let cache = BlockCache::new(capacity);
    for offset in 0..64 {
        cache.insert(key(1, offset), block(key(1, offset), 1000));
        assert!(cache.used_bytes() <= 8 * 1024, "at {offset}");
    }
    assert!(cache.get(key(1, 0)).is_none(), "the first block was kept");
    assert!(
        cache.get(key(1, 63)).is_some(),
        "the last block was dropped"
    );
}

#[test]
fn a_block_larger_than_the_cache_is_not_kept() {
    let cache = BlockCache::new(4096);
    cache.insert(key(1, 0), block(key(1, 0), 100));
    cache.insert(key(1, 1), block(key(1, 1), 10_000));
    assert!(cache.get(key(1, 1)).is_none());
    assert!(
        cache.get(key(1, 0)).is_some(),
        "the big block evicted a small one"
    );
}

#[test]
fn inserting_a_block_twice_charges_it_once() {
    let cache = BlockCache::new(1 << 20);
    cache.insert(key(1, 0), block(key(1, 0), 1000));
    let once = cache.used_bytes();
    cache.insert(key(1, 0), block(key(1, 0), 1000));
    assert_eq!(cache.used_bytes(), once);
}

#[test]
fn a_reader_keeps_its_block_after_the_cache_drops_it() {
    let cache = BlockCache::new(4096);
    cache.insert(key(1, 0), block(key(1, 0), 2000));
    let held = cache.get(key(1, 0)).unwrap();
    for offset in 1..16 {
        cache.insert(key(1, offset), block(key(1, offset), 2000));
    }
    assert!(cache.get(key(1, 0)).is_none());
    assert_eq!(named(&held), key(1, 0));
}

#[test]
fn concurrent_readers_and_writers_see_only_the_block_of_their_key() {
    let capacity = 64 * 1024;
    let cache = Arc::new(BlockCache::new(capacity));
    let threads: Vec<_> = (0..8_u64)
        .map(|thread| {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || {
                for step in 0..2_000_u64 {
                    let wanted = key(step % 7, (step * 31 + thread) % 97);
                    match cache.get(wanted) {
                        Some(found) => assert_eq!(named(&found), wanted),
                        None => cache.insert(wanted, block(wanted, 700)),
                    }
                    assert!(cache.used_bytes() <= capacity);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}
