# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Workspace with the `tessarikv`, `tessarikv-constants`, `tessarikv-types`, `tessarikv-sync` and
  `tessarikv-format` crates.
- Internal-key encoding: user key followed by a trailer of sequence number and value kind,
  ordered by user key ascending and then by sequence number descending.
- Append-only byte buffer with one writer and concurrent wait-free readers.
- `SkipList`: an insert-only lock-free skiplist with entries in a lock-free arena, ordered by a
  pluggable `KeyComparator`, with forward and backward cursors.
- `ClockCache`: a byte-bounded cache with wait-free lookups and inserts, evicting entries not read since the
  clock last passed them; a value read stays valid after its entry is evicted.
- `Sequencer`: lock-free sequence-number claims for concurrent commits, with a visible watermark that only covers
  completed batches.
- `WriteBatch`: an atomic, ordered list of puts and deletes across keyspaces, with a versioned encoding whose decoder
  refuses truncated, oversized or malformed input.
- Table files: `TableBuilder` writes data blocks, a bloom filter, an index, a properties block and a versioned footer,
  each block sealed with a CRC-32C and optionally LZ4-compressed; `Table` opens one and `TableIter` seeks and walks it,
  stopping with an error on a damaged block.
- Bloom filter blocked by cache line: one line per lookup, no false negatives, about 1 % false positives at ten bits
  per key; a filter whose size or probe count does not add up is refused.
- Table block format: sorted entries with shared key prefixes and restart points, a builder that refuses keys out of
  order, and an iterator that seeks by binary search and stops with an error on a damaged entry.
- Log frame codec: CRC-32C checksummed frames tagged with their log number; a reader returns the valid prefix and
  whether the log ended cleanly, with a torn write, or on bytes from an earlier use of the file.
- Write-ahead log: one writer task puts every logged batch in commit order into segment files,
  writes each run of waiting batches with one call and syncs once for all writers waiting on durability;
  a failed write or sync stops the log instead of reporting lost data as durable.
- `Db::open(path, options)` on the file system (or `Db::open_with_storage` on any `Storage`): creates or
  opens the database, holds its lock, and replays the log; `WalRecovery::PointInTime` cuts a damaged log
  back to its valid prefix, `WalRecovery::Absolute` refuses to open. `Db::close` syncs and releases it.
- `Db`: an in-memory database with async `open`, atomic batch `write`, `get` and paged `scan` at a
  consistent snapshot; a scan never shows part of a batch.
- `ManifestEdit`: a versioned record of tables added and removed, the flushed sequence and the next file
  number; unknown versions and fields, repeated fields and lengths past the record are refused.
- `Db::snapshot`: a `Snapshot` reads `get` and `scan` at a fixed point in history until it is dropped.
- Flush: on storage, an in-memory table that reaches `KeyspaceOptions::memtable_bytes` is written in the background
  to a table file, named in the manifest, and replaces the in-memory table for reads; the log segments it covers
  are removed. Opening reads the tables the manifest names, removes unnamed ones, and replays the log above them.
  `DbOptions::default_keyspace` sets the default keyspace's options. A failed flush stops writes with
  `Error::FlushFailed`.
- Iterators: `Db::iter` and `Snapshot::iter` walk a keyspace at one snapshot, forward (`seek`, `seek_to_first`,
  `next`) and backward (`seek_for_prev`, `seek_to_last`, `prev`), across in-memory tables and table files, inside
  `ReadOptions::lower_bound` / `upper_bound`;
  `ReadOptions::with_prefix` bounds one to a key prefix. An iterator keeps the tables it reads alive.
- `SkipList::cursor_at` resumes a cursor from a `CursorPosition` taken from the same list.
- Table and block iterators move backward: `seek_to_last`, `seek_for_prev` (tables) and `prev`.
- Range deletes: `WriteBatch::delete_range` removes every key in `[start, end)` written before it; point reads,
  scans and iterators honour it at every snapshot, and it survives flushes and reopening. The batch encoding moves to
  version 2 and table files to format version 2 (a range-delete block); version 1 of both is still read.
- Block cache: a table file keeps its index, filter and properties in memory and reads data blocks on demand
  through a cache of `DbOptions::block_cache_bytes` (zero disables it) whose lookups and inserts never wait, sized
  for the keyspace's block size; a block that is not cached is read on a
  blocking thread, and `ReadOptions::fill_cache = false` leaves the cache as it was. `Table::open_with` opens a
  table through positional reads with its data blocks served by a `BlockSource`, and `TableIter::pending` /
  `resume` let a walk wait for a block. A block read whose thread stops returns `Error::ReadFailed`.
- Write stall: full in-memory tables queue for their flushes, up to `KeyspaceOptions::max_immutable_memtables`; past
  that a write waits for the next flush, or returns `Error::WriteStalled` when `WriteOptions::no_slowdown` is set.
- Options: `DbOptions`, `KeyspaceOptions` with the `point_lookup`, `write_heavy` and `bulk_load`
  presets, `ReadOptions` and `WriteOptions` with three durability levels.
- `Storage` abstraction with an operating-system backend and an in-memory backend.
- Error type with categories, stable codes and a retryable flag.
