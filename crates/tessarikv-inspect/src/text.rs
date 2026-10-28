//! Bytes as text: printable ASCII as it is, everything else escaped.

use std::fmt::Write as _;

/// `bytes` with printable ASCII kept, a backslash doubled, and every other byte as `\xNN`.
pub fn escaped(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len());
    for &byte in bytes {
        match byte {
            b'\\' => text.push_str("\\\\"),
            b' '..=b'~' => text.push(char::from(byte)),
            _ => {
                let _ = write!(text, "\\x{byte:02x}");
            }
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::escaped;

    #[test]
    fn printable_bytes_stay_and_others_are_escaped() {
        assert_eq!(escaped(b"key-1"), "key-1");
        assert_eq!(escaped(b"a\\b"), "a\\\\b");
        assert_eq!(escaped(&[0, 0xff, b'\n']), "\\x00\\xff\\x0a");
    }
}
