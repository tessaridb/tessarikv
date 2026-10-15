//! Variable-length unsigned integers: seven bits per byte, low bits first, high bit set on
//! every byte but the last.

/// Appends `value` to `out`.
pub(crate) fn put_u32(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7F).unwrap_or(0) | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).unwrap_or(0));
}

/// The value encoded at the start of `bytes` and the bytes it took, or `None` when the input
/// ends first or the value does not fit in 32 bits.
pub(crate) fn get_u32(bytes: &[u8]) -> Option<(u32, usize)> {
    let mut value = 0_u32;
    for (index, byte) in bytes.iter().take(5).enumerate() {
        let shift = u32::try_from(index).ok()?.checked_mul(7)?;
        let bits = u32::from(byte & 0x7F);
        // The fifth byte may carry only the top four bits of a u32.
        if shift == 28 && bits > 0x0F {
            return None;
        }
        value |= bits.checked_shl(shift)?;
        if byte & 0x80 == 0 {
            return Some((value, index.checked_add(1)?));
        }
    }
    None
}

#[cfg(test)]
mod tests;
