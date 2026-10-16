//! Reads at an offset without a shared file cursor, so any number of threads read one handle.

use std::fs::File;

/// Fills `buf` from `offset` with `pread`.
#[cfg(unix)]
pub(super) fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

/// Fills `buf` from `offset` with positioned `ReadFile` calls.
#[cfg(windows)]
pub(super) fn read_exact_at(
    file: &File,
    mut offset: u64,
    mut buf: &mut [u8],
) -> std::io::Result<()> {
    while !buf.is_empty() {
        let read = std::os::windows::fs::FileExt::seek_read(file, buf, offset)?;
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let (_, rest) = std::mem::take(&mut buf).split_at_mut(read);
        buf = rest;
        offset = offset.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
    }
    Ok(())
}

/// Fills `buf` from `offset` with `preadv2(RWF_NOWAIT)`, which reads only what the page cache
/// already holds; `false` for a short read or any refusal, so the caller reads again where it
/// may wait and that read reports any real error.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) fn read_exact_at_nowait(file: &File, offset: u64, buf: &mut [u8]) -> bool {
    use rustix::io::{IoSliceMut, ReadWriteFlags, preadv2};
    let wanted = buf.len();
    let mut slices = [IoSliceMut::new(buf)];
    preadv2(file, &mut slices, offset, ReadWriteFlags::NOWAIT).is_ok_and(|read| read == wanted)
}

/// No read on this platform promises not to wait for the device.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(super) fn read_exact_at_nowait(_file: &File, _offset: u64, _buf: &mut [u8]) -> bool {
    false
}
