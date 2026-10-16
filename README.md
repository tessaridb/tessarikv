<div align="center">

<img src="assets/logo/tessarikv-mark-128.png" alt="" width="88" height="88">

# TessariKV

**The future heart of TessariDB: an embedded key-value storage engine in pure Rust, lock-free first.**

[![status](https://img.shields.io/badge/status-in%20development-D98E33?style=flat-square)](#status)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-6B5FD1?style=flat-square)](LICENSE)

</div>

TessariKV is an LSM tree written to become the storage layer under
[TessariDB](https://github.com/tessaridb/tessaridb). Every shared structure on a read or write path is lock-free or
wait-free wherever a non-blocking form exists, so any number of tasks write and read at once without a global lock.
It has no C or C++ dependency, exposes an async API on tokio, and runs on macOS, Linux and Windows.

## Status

Early development. Nothing is published to crates.io yet, and the on-disk format is not stable.

| Part | State |
|---|---|
| Writes | atomic batches of puts, deletes and range deletes through a write-ahead log; each write chooses its durability: synced, handed to the OS, or not logged; `sync_wal` makes everything written durable on demand |
| Reads | point reads, many-key reads and many-range scans at one snapshot, paged scans and bounded iterators at a consistent snapshot; snapshots held on demand, the oldest one reported; approximate range sizes from table indexes |
| Transactions | snapshot isolation (first committer wins, checked without locks) and opt-in serializable (reads and scanned ranges validated too): point reads and range scans at one snapshot plus the transaction's own writes, savepoints, an atomic commit |
| In-memory table | lock-free skiplist with a whole-key filter that lets reads of absent keys skip it; a database can also live entirely in memory; `DbOptions::total_memtable_bytes` caps the newest tables of every keyspace together |
| Tables | sorted blocks with prefix compression, an index, a bloom filter and a checksum on every block; LZ4 (default), Snappy, Zstandard or Deflate per keyspace, with a separate codec for the bottom level |
| Block cache | lock-free CLOCK cache, sized by `DbOptions::block_cache_bytes`; `DbOptions::max_open_files` bounds the table files kept open |
| Flush | background flush to table files named by a synced manifest; recovery replays only the log above them, flushes what it replayed and removes that log; `Db::flush` on demand; a failed flush, compaction or log write stops writes and `Db::background_error` reports it |
| Compaction | leveled (default), universal and FIFO; several compactions at once, each split into parallel parts; rate-limited output; `Db::compact_range` on demand |
| Backpressure | writes slow down, then stop, on level-0 size, pending compaction bytes and unflushed tables |
| Keyspaces | independent sets of keys, each with its own options, in-memory tables, levels and compactions; named at open, created or dropped while running (`Db::create_keyspace`, `Db::drop_keyspace`); a batch may span several and stays atomic across crashes |

## Example

<!-- non-executable example: the same code, inside an async function, is compiled as the crate's documentation example -->
```rust
use tessarikv::{Db, DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

let db = Db::open("example", DbOptions::in_memory()).await?;
let mut batch = WriteBatch::new();
batch.put(KeyspaceId::DEFAULT, b"apple", b"red")?;
db.write(&batch, &WriteOptions::default()).await?;
let value = db.get(KeyspaceId::DEFAULT, b"apple", &ReadOptions::default()).await?;
assert_eq!(value.as_deref(), Some(&b"red"[..]));
```

## Building

Rust 1.89 or newer.

```sh
cargo build --workspace
cargo test --workspace --tests
RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=3 cargo test -p tessarikv-sync --release --lib
```

The last line model-checks the lock-free building blocks under `loom`. Before a change goes in, `cargo fmt --all
--check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings` (also for
`x86_64-unknown-linux-gnu` and `x86_64-pc-windows-msvc`) and `cargo deny check` pass.

| Crate | Holds |
|---|---|
| `tessarikv` | the engine |
| `tessarikv-sync` | lock-free building blocks, each documented with its specification, linearization points and memory-ordering argument |
| `tessarikv-format` | on-disk log and table formats |
| `tessarikv-types` | sequence numbers, identifiers, value kinds, the internal-key codec |
| `tessarikv-constants` | defaults and limits, each with its unit and reason |

## Licence

Apache-2.0. See [LICENSE](LICENSE).
