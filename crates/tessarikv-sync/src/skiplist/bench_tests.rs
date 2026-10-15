//! Thread-scaling comparison of the skiplist against a `Mutex<BTreeMap>` behind the same
//! operations. Ignored by default; run with
//! `cargo test -p tessarikv-sync --release --lib scaling -- --ignored --nocapture`.

// Throughput and percentile arithmetic on sample counts converts integers to floating point.
#![allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

use super::{BytewiseComparator, SkipList};

/// The operations a benchmarked map offers.
trait Map: Send + Sync + 'static {
    fn put(&self, key: &[u8], value: &[u8]);
    fn read(&self, key: &[u8]) -> bool;
}

impl Map for SkipList<BytewiseComparator> {
    fn put(&self, key: &[u8], value: &[u8]) {
        black_box(self.insert(key, value).unwrap());
    }
    fn read(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }
}

impl Map for Mutex<BTreeMap<Vec<u8>, Vec<u8>>> {
    fn put(&self, key: &[u8], value: &[u8]) {
        self.lock()
            .unwrap()
            .entry(key.to_vec())
            .or_insert_with(|| value.to_vec());
    }
    fn read(&self, key: &[u8]) -> bool {
        self.lock().unwrap().contains_key(key)
    }
}

/// A 16-byte key from a 64-bit draw.
fn key(draw: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&draw.to_be_bytes());
    key[8..].copy_from_slice(&draw.rotate_left(29).to_be_bytes());
    key
}

fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Runs `ops` operations per thread; `read_percent` of them are reads of preloaded keys.
/// Returns (ops per second, p50, p99, p99.9 in nanoseconds of sampled operations).
fn run<M: Map>(
    map: Arc<M>,
    threads: usize,
    ops: u64,
    read_percent: u64,
    preloaded: u64,
) -> (f64, u64, u64, u64) {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let (map, barrier) = (Arc::clone(&map), Arc::clone(&barrier));
            std::thread::spawn(move || {
                let mut state = (thread as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                let mut samples = Vec::with_capacity(usize::try_from(ops / 16 + 1).unwrap());
                let value = [7_u8; 64];
                barrier.wait();
                for op in 0..ops {
                    let draw = next(&mut state);
                    let started = (op % 16 == 0).then(Instant::now);
                    if draw % 100 < read_percent {
                        black_box(map.read(&key(draw % preloaded.max(1))));
                    } else {
                        map.put(&key(draw), &value);
                    }
                    if let Some(started) = started {
                        samples
                            .push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                    }
                }
                samples
            })
        })
        .collect();
    barrier.wait();
    let started = Instant::now();
    let mut samples: Vec<u64> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let at = |fraction: f64| samples[((samples.len() - 1) as f64 * fraction) as usize];
    let total = (ops * threads as u64) as f64;
    (total / elapsed.as_secs_f64(), at(0.50), at(0.99), at(0.999))
}

fn preload<M: Map>(map: &M, keys: u64) {
    let value = [7_u8; 64];
    for draw in 0..keys {
        map.put(&key(draw), &value);
    }
}

fn curve(name: &str, read_percent: u64, preloaded: u64, ops: u64) {
    let cores = std::thread::available_parallelism().map_or(4, |count| count.get());
    println!(
        "\n## {name} (reads {read_percent}%, preloaded {preloaded}, {ops} ops/thread, 3 runs per point)"
    );
    println!(
        "| threads | skiplist Mops/s (min-max) | p50/p99/p99.9 ns | mutex Mops/s (min-max) | p50/p99/p99.9 ns |"
    );
    println!("|---|---|---|---|---|");
    for threads in [1, 2, 4, 8, cores, cores * 2] {
        let mut lists = Vec::new();
        let mut mutexes = Vec::new();
        for _ in 0..3 {
            let list = Arc::new(SkipList::new(BytewiseComparator));
            preload(&*list, preloaded);
            lists.push(run(list, threads, ops, read_percent, preloaded));
            let mutex = Arc::new(Mutex::new(BTreeMap::new()));
            preload(&*mutex, preloaded);
            mutexes.push(run(mutex, threads, ops, read_percent, preloaded));
        }
        let span = |runs: &[(f64, u64, u64, u64)]| {
            let min = runs.iter().map(|run| run.0).fold(f64::MAX, f64::min) / 1e6;
            let max = runs.iter().map(|run| run.0).fold(0.0, f64::max) / 1e6;
            let best = runs.iter().max_by(|a, b| a.0.total_cmp(&b.0)).unwrap();
            (
                format!("{min:.2}-{max:.2}"),
                format!("{}/{}/{}", best.1, best.2, best.3),
            )
        };
        let (list_rate, list_tail) = span(&lists);
        let (mutex_rate, mutex_tail) = span(&mutexes);
        println!("| {threads} | {list_rate} | {list_tail} | {mutex_rate} | {mutex_tail} |");
    }
}

#[test]
#[ignore = "benchmark: run explicitly in release"]
fn scaling() {
    // Warm-up: one discarded run per structure (allocator pools, page faults, caches).
    run(Arc::new(SkipList::new(BytewiseComparator)), 4, 50_000, 0, 0);
    run(Arc::new(Mutex::new(BTreeMap::new())), 4, 50_000, 0, 0);
    curve("insert only", 0, 0, 100_000);
    curve("read-mostly", 90, 200_000, 200_000);
}
