<div align="center">

<img src="assets/logo/tessarikv-mark-128.png" alt="" width="88" height="88">

# TessariKV

**The heart of TessariDB: an embedded key-value storage engine in pure Rust, lock-free first.**

[![crates.io](https://img.shields.io/crates/v/tessarikv?style=flat-square)](https://crates.io/crates/tessarikv)
[![docs.rs](https://img.shields.io/docsrs/tessarikv?style=flat-square)](https://docs.rs/tessarikv)
[![status](https://img.shields.io/badge/status-first%20release-2E9E6B?style=flat-square)](#status)
[![file format](https://img.shields.io/badge/file%20format-stable-2E9E6B?style=flat-square)](#file-format)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-6B5FD1?style=flat-square)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.89-6B5FD1?style=flat-square)](#building)

[kv.tessaridb.com](https://kv.tessaridb.com) · [docs](https://docs.tessaridb.com/kv) ·
[TessariDB](https://github.com/tessaridb/tessaridb)

</div>

TessariKV is the LSM tree under [TessariDB](https://github.com/tessaridb/tessaridb): every table, index, log and
catalog TessariDB keeps lives in it. Every shared structure on a read or write path is lock-free or
wait-free wherever a non-blocking form exists, so any number of tasks write and read at once without a global lock.
It has no C or C++ dependency, exposes an async API on tokio, and runs on macOS, Linux and Windows.

## Status

First release (0.1). The on-disk file format is stable from 0.1.0 (see [File format](#file-format)); the API may
still change in a minor release before 1.0.

| Part | State |
|---|---|
| Writes | atomic batches of puts, deletes, single deletes and range deletes through a write-ahead log; a put may expire at a time it names (`WriteBatch::put_expiring`), after which it reads as absent and compaction removes it; each write chooses its durability: synced, handed to the OS, or not logged; `sync_wal` makes everything written durable on demand |
| Reads | point reads, many-key reads and many-range scans at one snapshot, paged scans and bounded iterators at a consistent snapshot; snapshots held on demand, the oldest one reported; approximate range sizes from table indexes |
| Transactions | snapshot isolation (first committer wins, checked without locks) and opt-in serializable (reads and scanned ranges validated too): point reads and range scans at one snapshot plus the transaction's own writes, reads that hold their key for the commit (`get_for_update`, refused rather than waited for), savepoints, an atomic commit |
| In-memory table | lock-free skiplist with a whole-key filter that lets reads of absent keys skip it; a database can also live entirely in memory; `DbOptions::total_memtable_bytes` caps the newest tables of every keyspace together |
| Tables | sorted blocks with prefix compression, an index, a bloom filter and a checksum on every block; an optional prefix filter (`KeyspaceOptions::prefix_length`) lets a scan inside one key prefix pass over tables that hold none of it; LZ4 (default), Snappy, Zstandard or Deflate per keyspace, with a separate codec for the bottom level; values above a keyspace's `blob_threshold` live in blob files the table indexes, so compaction moves the index instead of the value; `Db::verify_checksums` re-reads every table and blob file from storage and verifies each block |
| Encryption | `DbOptions::encryption` with a caller's `KeyProvider`: every file holding data or metadata (tables, log, manifest, options record) is encrypted and authenticated (AES-256-GCM, or ChaCha20-Poly1305 without AES instructions; `DbOptionsBuilder::encryption_cipher` pins one) under a key of its own; a wrong or missing key is refused before any of them is read; to rotate, new files take the current key id, and an old key is needed until compaction has rewritten every file naming it |
| Statistics | `DbOptions::statistics` turns them on, `Db::statistics` reads them: tickers since open (point reads, in-memory table hits and misses, block cache hits and misses, block bytes read, filter outcomes, entries and bytes written, log bytes and syncs, stall time, flush bytes, compaction bytes read and written, entries compactions dropped, compactions that failed) and per-interval latency histograms for point reads and writes; each count is one wait-free add on a per-thread stripe; `Db::properties` reads a keyspace's files and bytes per level, what compaction owes and its in-memory bytes without walking the files; an `EventListener` is told of flushes, compactions, stall changes and the failure that stops the database |
| Block cache | lock-free CLOCK cache, sized by `DbOptions::block_cache_bytes`; `DbOptions::max_open_files` bounds the table files kept open; a block missing from the cache is read on the reading task's own thread (`DbOptions::block_reads_on_worker`, on by default) |
| Flush | background flush to table files named by a synced manifest, replaced by a fresh one past `DbOptions::max_manifest_bytes`; recovery replays only the log above them, flushes what it replayed and removes that log; `Db::flush` on demand; a failed flush, log write or manifest edit stops writes and `Db::background_error` reports it |
| Checkpoint | `Db::checkpoint` writes a consistent copy of a live database into another directory, holding every batch committed before the call; the copy opens as a database of its own; on a file system its table files are hard links, so it costs no table bytes |
| Read-only | `Db::open_read_only` reads a database as its files hold it, its log included, beside other readers and without a lock; it changes no file, and every write, flush, compaction or keyspace change is refused with `Error::ReadOnly` |
| Destroy | `Db::destroy` removes every file the engine wrote in a database's directory, in an order a crash cannot turn into a partial database; an open database is refused |
| Compaction | leveled (default), universal and FIFO; several compactions at once, each split into parallel parts; rate-limited output; a compaction that fails (a full disk, an unreadable input) removes the files it began, leaves its inputs in place and runs again after a growing wait, while writes go on; `Db::compact_range` on demand, `Db::wait_for_compact` until nothing is left to do; automatic compactions can be turned off per keyspace for bulk loads; a keyspace's `CompactionFilter` may remove values as compactions meet them, never one a held snapshot still reads |
| Backpressure | writes slow down, then stop, on level-0 size, pending compaction bytes and unflushed tables |
| Keyspaces | independent sets of keys, each with its own options, in-memory tables, levels and compactions; named at open, created or dropped while running (`Db::create_keyspace`, `Db::drop_keyspace`); a batch may span several and stays atomic across crashes; every open records the options it runs with in `OPTIONS-<n>` |

## File format

Every file carries its own format version and a checksum, and every reader treats stored bytes as hostile input.
On top of that, each database records one store format, at the start of every manifest:

- a release reads its own store format and every earlier one, from 0.1.0 on;
- a database written in a later format is refused at open with `Error::UnsupportedFormat` before its tables or
  logs are read, never reported as corruption;
- a release that writes a layout an older one cannot read raises the store format and says so in its changelog;
  metadata that older releases may skip is added without raising it;
- opening a database stamps the store format of the release that opened it, so take a `Db::checkpoint` before
  moving a database to a newer release.

Databases written by 0.0.x recorded no store format; they are format 0 and open in 0.1.0.

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
