use super::{Cipher, FileKind, FileSealer, HEADER_BYTES, RootKey, SealHeader, TAG_BYTES};
use crate::Error;

const ROOT: [u8; 32] = [7; 32];

fn header(cipher: Cipher, salt: u8) -> SealHeader {
    SealHeader {
        cipher,
        key_id: 3,
        salt: [salt; 32],
    }
}

fn sealer(cipher: Cipher, salt: u8, kind: FileKind) -> FileSealer {
    FileSealer::new(&RootKey::new(ROOT), &header(cipher, salt), kind)
}

fn unauthenticated<T: std::fmt::Debug>(result: crate::Result<T>) {
    assert!(
        matches!(result, Err(Error::Unauthenticated { .. })),
        "{result:?}"
    );
}

const CIPHERS: [Cipher; 2] = [Cipher::Aes256Gcm, Cipher::ChaCha20Poly1305];

#[test]
fn sealed_units_open_back_at_their_position() {
    for cipher in CIPHERS {
        let sealer = sealer(cipher, 1, FileKind::Table);
        for size in [0, 1, 15, 16, 17, 4096] {
            let plain: Vec<u8> = (0..size)
                .map(|byte| u8::try_from(byte % 251).unwrap())
                .collect();
            let sealed = sealer.seal(4096, &plain).unwrap();
            assert_eq!(sealed.len(), size + TAG_BYTES);
            if size > 0 {
                assert_ne!(sealed.get(..size), Some(&plain[..]), "encrypted");
            }
            assert_eq!(
                sealer.open(4096, &sealed).unwrap(),
                plain,
                "{cipher:?} {size}"
            );
        }
    }
}

/// Every single changed byte of a sealed unit is caught: the whole unit, not a sample.
#[test]
fn every_changed_byte_fails_authentication() {
    for cipher in CIPHERS {
        let sealer = sealer(cipher, 1, FileKind::Log);
        let sealed = sealer.seal(0, b"a write-ahead frame").unwrap();
        for index in 0..sealed.len() {
            let mut changed = sealed.clone();
            changed[index] ^= 0x01;
            unauthenticated(sealer.open(0, &changed));
        }
        unauthenticated(sealer.open(0, &sealed[..sealed.len() - 1]));
        unauthenticated(sealer.open(0, &[]));
    }
}

/// A unit is bound to its key, file, kind and position: moved anywhere else it fails.
#[test]
fn a_unit_moved_or_opened_with_another_key_fails() {
    for cipher in CIPHERS {
        let sealer = sealer(cipher, 1, FileKind::Table);
        let sealed = sealer.seal(64, b"block").unwrap();
        assert_eq!(sealer.open(64, &sealed).unwrap(), b"block");
        unauthenticated(sealer.open(128, &sealed));
        unauthenticated(self::sealer(cipher, 2, FileKind::Table).open(64, &sealed));
        unauthenticated(self::sealer(cipher, 1, FileKind::Manifest).open(64, &sealed));
        let other_root =
            FileSealer::new(&RootKey::new([8; 32]), &header(cipher, 1), FileKind::Table);
        unauthenticated(other_root.open(64, &sealed));
        let mut relabelled = header(cipher, 1);
        relabelled.key_id = 4;
        let relabelled = FileSealer::new(&RootKey::new(ROOT), &relabelled, FileKind::Table);
        unauthenticated(relabelled.open(64, &sealed));
    }
}

/// The same bytes at two positions, or in two files, encrypt differently: nonces never repeat.
/// The tag is left out of the comparison, since a reused nonce shows in the ciphertext alone.
#[test]
fn the_same_bytes_seal_differently_at_each_position_and_in_each_file() {
    for cipher in CIPHERS {
        let first = sealer(cipher, 1, FileKind::Table);
        let second = sealer(cipher, 2, FileKind::Table);
        let plain = [0_u8; 32];
        let body = |sealer: &FileSealer, position| {
            let mut sealed = sealer.seal(position, &plain).unwrap();
            sealed.truncate(plain.len());
            sealed
        };
        assert_ne!(body(&first, 0), body(&first, 32), "{cipher:?}");
        assert_ne!(body(&first, 0), body(&second, 0), "{cipher:?}");
        assert_eq!(
            body(&first, 0),
            body(&first, 0),
            "deterministic per position"
        );
    }
}

#[test]
fn headers_round_trip_and_hostile_ones_are_refused() {
    for cipher in CIPHERS {
        let header = header(cipher, 9);
        let bytes = header.encode();
        assert_eq!(bytes.len(), HEADER_BYTES);
        assert_eq!(SealHeader::decode(&bytes).unwrap(), header);
        let mut bad_magic = bytes;
        bad_magic[0] ^= 0xff;
        assert!(matches!(
            SealHeader::decode(&bad_magic),
            Err(Error::Malformed { .. })
        ));
        let mut bad_version = bytes;
        bad_version[4] = 99;
        assert!(matches!(
            SealHeader::decode(&bad_version),
            Err(Error::UnsupportedVersion { version: 99, .. })
        ));
        let mut bad_cipher = bytes;
        bad_cipher[5] = 0;
        assert!(matches!(
            SealHeader::decode(&bad_cipher),
            Err(Error::Malformed { .. })
        ));
        assert!(matches!(
            SealHeader::decode(&bytes[..HEADER_BYTES - 1]),
            Err(Error::Truncated { .. })
        ));
    }
    let fresh = SealHeader::generate(Cipher::ChaCha20Poly1305, 1).unwrap();
    let again = SealHeader::generate(Cipher::ChaCha20Poly1305, 1).unwrap();
    assert_ne!(
        fresh.salt, again.salt,
        "salts come from the operating system's random source"
    );
}

/// Pins the sealed format: a change to derivation, nonce, associated data or layout shows here.
#[test]
fn the_sealed_format_is_stable() {
    let sealed = sealer(Cipher::ChaCha20Poly1305, 1, FileKind::Table)
        .seal(4096, b"tessarikv")
        .unwrap();
    let hex: String = sealed.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(hex, GOLDEN_CHACHA);
    let sealed = sealer(Cipher::Aes256Gcm, 1, FileKind::Table)
        .seal(4096, b"tessarikv")
        .unwrap();
    let hex: String = sealed.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(hex, GOLDEN_AES);
}

const GOLDEN_CHACHA: &str = "ba90d0d237a40f0480efb58ab75f3a1cef31a447b3419fa4d2";
const GOLDEN_AES: &str = "bfd2e6e3a19ab038e548613616931b936cf8af1e5dfe367df6";
