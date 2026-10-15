//! Lock-free bump allocation for a memtable: node records in a byte arena and link towers in a
//! link arena. Nothing allocated here is freed before the arena itself is dropped.
//!
//! Sequential specification: `allocate(n)` returns a reference to `n` fresh bytes (or slots) that
//! no other allocation overlaps; `resolve(r)` / `slot(i)` returns the storage behind a reference.
//!
//! | Operation | Linearization point | Progress |
//! |---|---|---|
//! | `allocate` | the reserving `fetch_add` on the cursor (oversize: on the oversize counter) | lock-free: a reservation crossing a chunk end is wasted and retried; installing a chunk is a CAS whose loser frees only its own chunk |
//! | `resolve` / `slot` | the Acquire load of the chunk pointer | wait-free |
//!
//! Ordering: cursors are Relaxed: they hand out disjoint ranges and publish nothing; chunks are
//! published by the `SlotDirectory` (Release install, Acquire load). The bytes an allocation's
//! owner writes are published by whatever the owner publishes the reference with (the skiplist's
//! linking CAS).
//!
//! Reclamation: no allocation is freed or reused while the arena is shared, so a reference stays
//! valid and never names other bytes (no ABA). Dropping the arena frees every chunk.

mod bytes;
mod links;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

pub(crate) use bytes::ByteArena;
pub(crate) use links::LinkArena;
