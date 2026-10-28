# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `Db::changed_options` and `OptionChange`: each option an open runs with whose recorded value differs from the
  previous open's options record, compared as text; empty in memory, read-only, or when nothing changed.
- `ReadableFile::read_vec_at` and `ReadableFile::try_read_vec_at` read a block into a buffer of its own. Their
  defaults use `read_at` and `try_read_at`; the OS storage reads into memory that is not zeroed first.
- `WritableFile::syncer` and the `FileSyncer` trait: a handle that syncs a file from another thread while appends
  go on. The default is `None`, and the log then syncs as before.
- `WriteBatch::put_expiring`: a put that reads as absent from the time it names on, hiding the older versions of
  its key as a delete would; compaction writes it out as a delete once that time has passed.
- `CompactionFilter`, `FilterDecision` and `KeyspaceOptionsBuilder::compaction_filter`: compaction asks a
  keyspace's filter about the newest value of each key it writes that no held snapshot can read, and a removed value
  becomes a delete at its version. The options record names the filter.
- `Db::properties` and `KeyspaceProperties`: a keyspace's files and bytes per level, the bytes compaction owes, an
  estimate of its live bytes and its in-memory table bytes, read without walking the table files.
- `EventListener` and `DbOptionsBuilder::event_listener`: the engine reports flushes and compactions once installed
  (`FlushCompleted`, `CompactionCompleted`), each change of what it asks of writers (`StallCondition`), and, once,
  the failure that stops the database.
- `DbOptionsBuilder::block_reads_on_worker`: whether a block missing from the cache is read on the reading task's own
  thread. On by default — a read of many tasks keeps a short tail (p99.9 about 9× lower at 16 readers) — at the cost
  of holding the worker for a device read; off hands such reads to the runtime as before.
- `KeyspaceOptionsBuilder::prefix_length`: tables of the keyspace keep a second filter over the first bytes of their
  keys, and a scan or iterator whose bounds lie inside one such prefix passes over every table that holds none of
  it (about 10× more scans of an absent prefix over 19 level-0 tables). Reads spanning prefixes read as before.
  `Table::may_hold_prefix_range` answers for one table; a table written before this filter existed has none and is
  read as before.
- `KeyspaceOptionsBuilder::blob_threshold` and `blob_gc_age_cutoff_percent`: values at least that large move to blob
  files when tables are written, and the table keeps a 24-byte index to each, so compaction moves the index instead of
  the value (writes to tables and blob files fell from 3.9× to 1.3× the bytes written, 4 KiB values overwritten
  three times). Compaction moves the values of the oldest blob files into new ones as it passes them, and a blob file
  is removed once no table refers to it. Checkpoints, `Db::destroy` and `Db::verify_checksums` cover blob files;
  `Table::blob_files` names those a table refers to.
- `Transaction::get_for_update`: a read that holds its key for the commit — a write to it committed after the
  transaction began refuses the commit with `Conflict`, as for a key the transaction writes. Nothing waits.
- `WriteBatch::single_delete`: the delete of a key put exactly once; it reads as a delete.
- `Db::wait_for_compact`: returns once no flush or compaction is running, queued or due.
- `WriteBatch::merge`, `MergeOperator` and `KeyspaceOptionsBuilder::merge_operator`: a merge adds an operand to a
  key, and every read combines the operands above the key's last put or delete through the keyspace's operator.
  A merge to a keyspace without one is refused; operands the operator cannot combine read as `Error::MergeFailed`
  (code `merge_failed`).
- `Db::set_keyspace_options`: replaces a keyspace's options while the database runs; work starting afterwards
  follows them. A change of compaction style or of in-memory table sizes is refused.
- `tessarikv-inspect`, a command-line tool: `manifest`, `table` and `scan` print a database's manifest, a table
  file's entries and its live keys, changing no file.
- `FilterPolicy` and `KeyspaceOptionsBuilder::filter_policy`: a keyspace's tables can keep a ribbon filter in place
  of the bloom filter — at ten bits per key about three quarters of the bytes (7.6 bits per key) with 0.77 % of
  absent keys passing against 0.99 %, at a higher cost to build. Tables already written keep their filter.
- `TableWriter` and `Db::ingest`: tables of puts and deletes built outside a database are added to a keyspace
  whole, at one new sequence number newer than every write that returned before and older than every write after;
  they are checked first and written again under the keyspace's options. Writes made meanwhile return once it is done.
- `Db::open_read_only`: opens a database without a lock and reads it as its files hold it, the batches in its
  log included; it changes no file, and every change is refused with the new `Error::ReadOnly` (code `read_only`).

### Changed

- An iterator reads the table files of each level through the view it was created on instead of copying the
  level's list, so creating one costs the same however many files a level holds; scans from many tasks at once scale
  further (100-entry scans at 4 readers about 1.5× faster on a store of one million keys).
- Opening a keyspace that holds tables below level 0 with `CompactionStyle::Fifo` is refused with
  `Error::InvalidOptions` for `keyspace.compaction`: first-in-first-out compaction never removes those tables. Every
  other change of compaction style between opens is allowed.
- The store format is 4, the write batch encoding version 4 and the table format 4, for expiring puts, blob files,
  merge operands and ribbon filters. Format 1 to 3 databases open and are stamped 4; releases before this one refuse
  a format 4 database at open.
- A point read that misses the block cache reads the block without zeroing a buffer first, and an uncompressed
  block becomes the cached block without being copied. A seek inside a block compares restart keys in place.
- A `Durability::Buffered` write whose log frames fit the open segment is appended by the log task itself instead of
  on a blocking thread; a write that syncs, starts a new segment or removes one is still handed over.
- A point read builds the key it seeks in table files once, not once per file it consults, and a table seek copies
  its target only when it has to wait for a block.
- On Linux a `Durability::Sync` write that arrives while the log is syncing starts a sync of its own instead of
  waiting for that one to finish; up to four run at once, and a write is acknowledged only after every earlier sync
  succeeded. macOS and Windows keep one sync at a time.

### Fixed

- A table block that claims to decompress to more than any data block can hold (the largest block size plus one
  entry with the largest key and value) is refused before anything is allocated; a crafted Zstandard or Deflate
  block could otherwise make a read allocate up to 4 GiB.

## [0.1.2] - 2026-10-27

### Changed

- Scans and iterators read each level below 0 as one ordered source and open only the table files their range or
  bounds reach, so a positioned read touches one file per level instead of every file of the keyspace.
- A point read beside range deletes keeps its usual path through the table filters: it finds the newest range delete
  covering the key and reads the key as deleted when its newest version is older. Before, any range delete in a
  keyspace made every point read merge all of its sources. A table file's range deletes are read once when it opens
  and cut into fragments once per view, not on every read.

- A compaction that fails — an output that cannot be written or synced, an input that cannot be read — no longer
  stops writes. It removes the files it began, leaves its inputs as they were and runs again after a wait that starts
  at 100 ms and doubles up to 30 s; `Db::compact_range` returns the error of the compaction it waited for. Failures
  are counted by the new `Ticker::CompactionFailures`. A failed manifest edit still stops writes.

- A range delete committed after a transaction began refuses it only when its range may cover a key the transaction
  writes, or reads under serializable isolation, in the same keyspace. Before, any range delete in any keyspace
  refused every transaction begun before it. A transaction's own range delete is refused by a later write into its
  range (with `serializable_transactions`) or a later range delete of its keyspace, as before by any later commit
  without it.

- A flush releases the log segment it emptied: the segment being written is closed and removed once every frame in
  it is held by a table, so the log no longer keeps flushed writes until it reaches its size limit, and opening
  replays less.
- `Db::checkpoint` links the table files into the copy where the storage can, instead of copying their bytes.

### Added

- `Storage::link`, which gives a file that is never written again a second name; it returns `false` by default, and
  the operating-system storage makes a hard link.

### Fixed

- Range deletes now free their space: a compaction drops the versions a newer range delete hides when no snapshot
  can still read them, and drops the range delete itself once no other table file can hold a version under it.
  Before, every range delete and everything it covered stayed in the table files for good.

## [0.1.1] - 2026-10-24

### Fixed

- `Db::write` and `Transaction::commit` return only once the batch is visible to reads. Before, a write that
  finished while an earlier batch was still being applied could return first, and a read issued right after it
  missed it. A writer now waits for the earlier batches; with nothing earlier pending the check costs one load.

## [0.1.0] - 2026-10-17

The first release with a stable on-disk file format: a database written by 0.1.0 opens in every later release.

### Added

- Store format: every manifest begins with the store format of its database (`STORE_FORMAT`, now 1), and an open
  refuses a database written in a later format with `Error::UnsupportedFormat` (code `unsupported_format`) before
  it reads any of its tables or logs. A database from 0.0.x, which recorded none, is format 0 and opens.
- Manifest edits may carry fields marked safe to skip, so a later release can add metadata without locking this
  one out.

### Changed

- Manifest edits move to format version 2, which starts with the store format; version 1 edits are still read.
  A database opened by this release no longer opens in 0.0.x; take a `Db::checkpoint` before an upgrade.

### Fixed

- A manifest holding no readable edit is refused as corruption; before, it opened as an empty database.

## [0.0.2] - 2026-10-17

### Fixed

- A database dropped without `close` kept its directory locked until its running flush and
  compaction finished; before, another database could open the directory while they still wrote it.

## [0.0.1] - 2026-10-17

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
  each block sealed with a CRC-32C and compressed with LZ4, Snappy, Zstandard or Deflate when that saves an
  eighth; `Table` opens one and `TableIter` seeks and walks it,
  stopping with an error on a damaged block.
- In-memory tables carry a lock-free whole-key filter sized from `KeyspaceOptions::memtable_bytes`, so a read of a
  key the table does not hold skips its walk.
- Bloom filter blocked by cache line: one line per lookup, no false negatives, about 1 % false positives at ten bits
  per key and about 0.02 % at twenty; a filter whose size or probe count does not add up is refused. Table files
  move to format version 3, whose filters pick each probe independently; versions 1 and 2 are still read.
- Table block format: sorted entries with shared key prefixes and restart points, a builder that refuses keys out of
  order, and an iterator that seeks by binary search and stops with an error on a damaged entry.
- Log frame codec: CRC-32C checksummed frames tagged with their log number (the checksum covers the payload first,
  so the log writer finishes it by appending twelve bytes); a reader returns the valid prefix and
  whether the log ended cleanly, with a torn write, or on bytes from an earlier use of the file.
- Write-ahead log: one writer task puts every logged batch in commit order into segment files,
  writes each run of waiting batches with one call and syncs once for all writers waiting on durability;
  a failed write or sync stops the log instead of reporting lost data as durable.
- `Db::open(path, options)` on the file system (or `Db::open_with_storage` on any `Storage`): creates or
  opens the database, holds its lock, and replays the log; `WalRecovery::PointInTime` cuts a damaged log
  back to its valid prefix, `WalRecovery::Absolute` refuses to open. What the replay recovers is flushed to a
  table before the open returns and its log removed, so no log outlives a reopen. `Db::close` syncs and releases it.
- `Db`: an in-memory database with async `open`, atomic batch `write`, `get` and paged `scan` at a
  consistent snapshot; a scan never shows part of a batch.
- `ManifestEdit`: a versioned record of tables added and removed, keyspaces added (`KeyspaceRecord`) and dropped,
  the flushed sequence and the next file and keyspace numbers; unknown versions and fields, repeated fields, lengths
  past the record and keyspace names that are not UTF-8 are refused.
- `Db::snapshot`: a `Snapshot` reads `get` and `scan` at a fixed point in history until it is dropped.
- `Db::begin`: a `Transaction` reads at the snapshot it began at and sees its own puts, deletes and range deletes,
  rolls back to a `Savepoint`, and commits everything as one atomic batch; dropping it writes nothing. New errors
  `Error::TooLarge` and `Error::InvalidArgument`.
- `Db::flush`: writes every batch committed before the call to table files and returns once they are there.
- `Db::compact_range`: compacts a key range down to the bottom level and returns once done; versions no snapshot
  reads and deletes leave the disk. Writes, reads and automatic compactions go on meanwhile.
- `Db::approximate_size`: the bytes of table files a key range takes, from table metadata and indexes, reading
  nothing from storage; `Table::offset_of` places a key in a table file from its index.
- `multi_get` on `Db`, `Snapshot` and `Transaction`: many keys read at one snapshot, values in the order asked.
- Every open on storage records the options it runs with in `OPTIONS-<n>` beside the data (the two newest are
  kept), for audits; the engine never reads it back.
- `FileSealer`: authenticated encryption of the units a file is written in, with AES-256-GCM or
  ChaCha20-Poly1305, a key per file derived from the caller's root key, and each unit bound to its file,
  kind and position. `TableBuilder::sealed` and `Table::open_sealed` write and read tables whose blocks and
  footer are sealed, so a table opens only with its key and only whole.
- `DbOptions::encryption`: table files written by flushes and compactions are encrypted under a key of their own,
  derived from a root key a caller's `KeyProvider` hands out by id; a key-check file makes an open refuse a wrong
  key (`Error::WrongKey`), a missing one (`Error::KeyNotFound`), or a key for a database written without one.
  The write-ahead log is encrypted too: each writer seals its own batch, and a batch changed on storage fails
  the open in every recovery mode. Manifests and the options record are encrypted as well, each manifest edit
  bound to its place in the file, and the key is checked before the manifest is read. Keys rotate by id: new
  files take the current one, and an old key is needed only until no file names it.
- `KeyspaceOptions::disable_auto_compactions`: the engine starts no compaction of the keyspace by itself, for
  bulk loads; `Db::compact_range` still runs.
- `Db::destroy` / `Db::destroy_with_storage`: removes every file the engine wrote, log first and identity last, so
  a crash part way never leaves a database holding part of the old data; refused while the database is open.
- `Db::checkpoint`: writes a consistent copy of a live database into another directory (flushed first, table files
  kept from removal while copied, a manifest of that moment); the copy opens as a database of its own.
- `Db::verify_checksums`: re-reads every table file from storage, bypassing the block cache, and verifies its
  footer, index, filter and every data block; the first damaged file is reported as `Error::Corruption`.
- `DbOptions::max_manifest_bytes` (default 64 MiB): past it the manifest is replaced by a new one holding the
  current set of tables as one change, so an open does not read an unbounded history.
- `DbOptionsBuilder::encryption_cipher`: pins the cipher new files are encrypted with (`Cipher`); files written
  under another cipher still open.
- `StripedCounters`: counters split over per-thread 128-byte stripes, with wait-free adds, sums exact once adds
  stop, and takes that reset a counter without losing an add.
- `DbOptions::statistics` and `Db::statistics`: tickers since open (`Ticker`) for point reads, in-memory table
  hits and misses, block cache hits and misses, block bytes read, filter outcomes, entries and bytes written,
  log bytes and syncs, stall time, flush bytes, compaction bytes read and written and entries compactions
  dropped; point-read and write latency `Histogram`s covering the interval since the previous read.
- `DbOptions::max_open_files`: keeps at most that many table files open (zero, the default, keeps every one);
  a read of a file let go opens it again, and a removed file lets go of its handle.
- Opening refuses a log whose segments skip a number (a segment removed from outside), in every recovery mode,
  instead of replaying around the gap.
- `DbOptions::total_memtable_bytes`: one ceiling over the newest in-memory tables of every keyspace together;
  reaching it freezes and flushes them as if one were full (zero, the default, sets none).
- `Db::background_error`: the failure that stopped background work (log, flush or compaction), read without a
  write.
- `Db::sync_wal`: makes every batch written before the call durable, buffered ones included, with one log sync.
- `multi_scan` on `Db`, `Snapshot` and `Transaction`: a page for each of many `ScanRange`s, all at one snapshot,
  in the order asked.
- `Db::oldest_snapshot`: the oldest snapshot a `Snapshot` or `Transaction` still holds, which keeps old versions
  from compaction.
- Serializable transactions: `DbOptions::serializable_transactions` (off by default) lets `Db::begin_with` run a
  transaction with `Isolation::Serializable`; its commit is also refused when a key it read or a range it scanned
  was written after it began. `Transaction::get` and `scan` now take `&mut self`.
- `Transaction::scan`: pages of a range at the transaction's snapshot with its own puts, deletes and range deletes
  applied, paged like `Db::scan`.
- Snapshot isolation: `Transaction::commit` is refused with `Error::Conflict` (retryable) when a write committed after
  the transaction began, by a transaction or a plain write, touched a key it writes. Write skew is admitted; range
  deletes are checked conservatively.
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
  for the keyspace's block size; a block that is not cached is read on the caller's thread when the storage can
  read it without blocking (`ReadableFile::try_read_at`: always for memory, pages the file system already holds on
  Linux), otherwise in place on a multi-thread runtime's worker (its other tasks move to another thread) and on a
  blocking thread elsewhere, and `ReadOptions::fill_cache = false` leaves the cache as it was. `Table::open_with` opens a
  table through positional reads with its data blocks served by a `BlockSource`, and `TableIter::pending` /
  `resume` let a walk wait for a block. A block read whose thread stops returns `Error::ReadFailed`.
- Leveled compaction: level 0 goes whole into the base level once it holds
  `KeyspaceOptions::level0_compaction_trigger` tables, and a level over its target (computed from the bottom level up
  with `level_base_bytes` and `level_size_multiplier`) moves its least-overlapping table down; outputs close at
  `target_file_bytes` between keys. A compaction keeps the newest version per snapshot, drops a delete nothing below
  needs, carries range deletes, and removes its inputs once no reader holds them. A point read consults only the
  tables whose range holds its key. `Error::CompactionFailed` stops writes like a failed flush.
  Up to `KeyspaceOptions::max_compactions` (4) compactions run at once, none reading a table another
  reads or writing a range another writes in the same level, and each splits into up to
  `KeyspaceOptions::max_subcompactions` (4) key ranges merged on their own threads.
  `DbOptions::compaction_bytes_per_second` paces compaction writes (a second's worth at once);
  flushes are not paced.
- First-in-first-out compaction: `KeyspaceOptions::compaction(CompactionStyle::Fifo { max_table_bytes })` keeps tables
  in level 0, never merges them, and drops the oldest once they hold more than `max_table_bytes` (1 GiB by default);
  a key reads its newest kept value or nothing. Level-0 stalls do not apply to it.
- Tables are written while they are built: `TableBuilder::take_finished` hands out the finished bytes and
  `TableBuilder::drained` says where the rest begins, so flushes and compactions hold about 1 MiB of a table
  (`TABLE_WRITE_CHUNK_BYTES`) plus its index and filter in memory, whatever the table's size; paced compactions
  pace each piece.
- Universal compaction: `CompactionStyle::Universal` (or `CompactionStyle::universal()`) treats each level-0 table and
  the bottom level as sorted runs; the newest runs merge by size ratio (or, past the trigger, the adjacent runs with the
  fewest bytes among those enough to go under it), and everything merges into the bottom once the runs above it hold `max_size_amplification_percent` of it.
- Table compression per keyspace: `KeyspaceOptions::compression` (LZ4 by default) for flushes and compactions, and
  `KeyspaceOptions::bottommost_compression` for tables written to the bottom level; Deflate levels above 10 are refused.
- Level-0 stalls: `KeyspaceOptions::level0_slowdown_trigger` (20) paces writes to
  `DbOptions::delayed_write_bytes_per_second` (16 MiB/s) and
  `level0_stop_trigger` (36) holds writes until a compaction takes level 0 under it; `no_slowdown` writes are refused
  with `Error::WriteStalled` naming the trigger. Writers see `Error::CompactionFailed` after a failed compaction.
  The bytes compaction owes, estimated after every install, slow writes the same way past
  `KeyspaceOptions::soft_pending_compaction_bytes` (64 GiB) and hold them past `hard_pending_compaction_bytes` (256 GiB).
- Write stall: full in-memory tables queue for their flushes, up to `KeyspaceOptions::max_immutable_memtables`; past
  that a write waits for the next flush, or returns `Error::WriteStalled` when `WriteOptions::no_slowdown` is set.
- Options: `DbOptions`, `KeyspaceOptions` with the `point_lookup`, `write_heavy` and `bulk_load`
  presets, `ReadOptions` and `WriteOptions` with three durability levels.
- `Storage` abstraction with an operating-system backend and an in-memory backend.
- Error type with categories, stable codes and a retryable flag.
- Keyspaces: `DbOptionsBuilder::keyspace` names keyspaces to open (`create_missing_keyspaces` adds those the
  database lacks), `Db::create_keyspace` and `Db::drop_keyspace` add and remove one while the database runs,
  `Db::keyspace` and `Db::keyspaces` look them up. Each keyspace flushes, compacts and paces writes by its own options; a batch spanning keyspaces is
  recovered whole or not at all.

[Unreleased]: https://github.com/tessaridb/tessarikv/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/tessaridb/tessarikv/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/tessaridb/tessarikv/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tessaridb/tessarikv/compare/v0.0.2...v0.1.0
[0.0.2]: https://github.com/tessaridb/tessarikv/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/tessaridb/tessarikv/releases/tag/v0.0.1
