//! A growable byte buffer with one appender and any number of concurrent readers.

mod directory;
mod reader;
mod writer;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

pub use reader::AppendReader;
pub use writer::AppendWriter;

use crate::error::BufferError;
use crate::shim::Arc;
use directory::Shared;

/// A new empty buffer whose bytes are held in chunks of `chunk_bytes`: one appender and any
/// number of concurrent readers.
///
/// # Contract
///
/// Sequential specification: `append(b)` makes the content `content ++ b`; `len()` returns the
/// content's length; `read_at(o, buf)` fills `buf` with `content[o..o + buf.len()]`, or refuses
/// with [`BufferError::OutOfRange`] when that range is not all published. Linearizable.
///
/// | Operation | Linearization point | Progress |
/// |---|---|---|
/// | `append` | the Release store of `published` | wait-free (allocation aside) |
/// | `len` | the Acquire load of `published` | wait-free |
/// | `read_at` | its Acquire load of `published` | wait-free |
///
/// Ordering: the writer installs blocks and chunks with Release stores, copies the bytes, then
/// stores the new length with Release; a reader's Acquire load of the length therefore sees
/// every byte and pointer below it. The writer only writes bytes at or above the published
/// length and readers only read below it, so the two never touch the same byte.
///
/// Reclamation: nothing is freed or replaced while any handle lives (no reuse, so no ABA); the
/// last handle dropped frees every block and chunk.
///
/// Cardinality is in the types: [`AppendWriter`] is not `Clone`, [`AppendReader`] is.
///
/// # Errors
///
/// [`BufferError::Overflow`] when `chunk_bytes` is zero.
pub fn append_buffer(chunk_bytes: usize) -> Result<(AppendWriter, AppendReader), BufferError> {
    if chunk_bytes == 0 {
        return Err(BufferError::Overflow);
    }
    let shared = Arc::new(Shared::new(chunk_bytes));
    let reader = AppendReader::new(Arc::clone(&shared));
    Ok((AppendWriter::new(shared), reader))
}

/// Splits a byte position into (chunk index, offset in chunk).
fn position(at: u64, chunk_bytes: usize) -> Result<(u64, usize), BufferError> {
    let chunk = u64::try_from(chunk_bytes).map_err(|_| BufferError::Overflow)?;
    let index = at.checked_div(chunk).ok_or(BufferError::Overflow)?;
    let within = at.checked_rem(chunk).ok_or(BufferError::Overflow)?;
    Ok((
        index,
        usize::try_from(within).map_err(|_| BufferError::Overflow)?,
    ))
}
