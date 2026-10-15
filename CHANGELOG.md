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
- Log frame codec: CRC-32C checksummed frames tagged with their log number; a reader returns the valid prefix and
  whether the log ended cleanly, with a torn write, or on bytes from an earlier use of the file.
- `Db`: an in-memory database with async `open`, atomic batch `write`, `get` and paged `scan` at a
  consistent snapshot; a scan never shows part of a batch.
- Options: `DbOptions`, `KeyspaceOptions` with the `point_lookup`, `write_heavy` and `bulk_load`
  presets, `ReadOptions` and `WriteOptions` with three durability levels.
- `Storage` abstraction with an operating-system backend and an in-memory backend.
- Error type with categories, stable codes and a retryable flag.
