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
- `WriteBatch`: an atomic, ordered list of puts and deletes across keyspaces, with a versioned encoding whose decoder
  refuses truncated, oversized or malformed input.
- Options: `DbOptions`, `KeyspaceOptions` with the `point_lookup`, `write_heavy` and `bulk_load`
  presets, `ReadOptions` and `WriteOptions` with three durability levels.
- `Storage` abstraction with an operating-system backend and an in-memory backend.
- Error type with categories, stable codes and a retryable flag.
