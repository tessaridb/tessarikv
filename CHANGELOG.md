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
- `Db::snapshot`: a `Snapshot` reads `get` and `scan` at a fixed point in history until it is dropped.
- Options: `DbOptions`, `KeyspaceOptions` with the `point_lookup`, `write_heavy` and `bulk_load`
  presets, `ReadOptions` and `WriteOptions` with three durability levels.
- `Storage` abstraction with an operating-system backend and an in-memory backend.
- Error type with categories, stable codes and a retryable flag.
