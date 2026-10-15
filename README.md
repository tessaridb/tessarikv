# TessariKV

An embedded key-value storage engine written in pure Rust: an LSM tree whose shared structures
are lock-free or wait-free wherever a non-blocking form exists. It is being built as the storage
layer for [TessariDB](https://github.com/tessaridb/tessaridb), with no C or C++ dependency.

**Status: early development.** `Db` opens on the file system or in memory, writes batches
atomically through a write-ahead log, and answers point reads and paged scans at a consistent
snapshot, from any number of concurrent tasks; `Db::snapshot` holds a point in history that later
reads return to. A write returns once it is as durable as it asks:
synced (survives a power loss), handed to the operating system (survives a process crash), or not
logged. On storage, a full in-memory table is flushed in the background to a table file named by
a synced manifest, and the log segments it covers are removed; opening reads the manifest and its
table files and replays only the log above them. Writes wait while the in-memory tables
awaiting flushes reach the keyspace's limit. Underneath it:

- the internal-key encoding and its ordering (`tessarikv-types`);
- an append-only byte buffer with one writer and any number of concurrent readers, and an
  insert-only lock-free skiplist for the in-memory table, and a lock-free sequencer whose visible
  watermark covers only completed commits, all model-checked with `loom` (`tessarikv-sync`);
- the write batch, the checksummed log frame, the manifest edit, and the table file: sorted data blocks with shared
  key prefixes, an index, a cache-line bloom filter, optional LZ4, and a checksum on every block
  verified on every read (`tessarikv-format`);
- typed, validated options for the database, for each keyspace, and for each read and write;
- a file-system abstraction with an operating-system backend and a fully in-memory one;
- the engine's error type, with stable codes and categories.

Nothing here is published to crates.io yet, and the on-disk format is not stable.

## Platforms

macOS, Linux and Windows. Every change is linted for all three targets. A durable flush is
`F_FULLFSYNC` on macOS, `fdatasync` on Linux and `FlushFileBuffers` on Windows.

## Building

Requires Rust 1.89 or newer (`rust-toolchain.toml` selects stable).

```sh
cargo build --workspace
cargo test --workspace --tests
```

The concurrency models run under `loom`:

```sh
RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=3 cargo test -p tessarikv-sync --release --lib
```

## Development

Before a change is committed:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy --workspace --all-targets --all-features --target x86_64-unknown-linux-gnu -- -D warnings
cargo clippy --workspace --all-targets --all-features --target x86_64-pc-windows-msvc -- -D warnings
cargo test --workspace --tests
cargo deny check
```

The cross-target lints need `rustup target add x86_64-unknown-linux-gnu x86_64-pc-windows-msvc`.

| Crate | Holds |
|---|---|
| `tessarikv-constants` | defaults and limits, each with its unit and reason |
| `tessarikv-types` | sequence numbers, identifiers, value kinds, the internal-key codec |
| `tessarikv-sync` | lock-free building blocks, each documented with its specification, linearization points and memory-ordering argument |
| `tessarikv-format` | on-disk log and table formats |
| `tessarikv` | the engine |

## License

Apache-2.0. See [LICENSE](LICENSE).
