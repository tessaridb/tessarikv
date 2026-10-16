use proptest::prelude::*;

use super::Compression;
use super::codec::{compress, decompress};
use crate::varint;

/// Every way a table can store its data blocks.
pub(super) const EVERY: [Compression; 5] = [
    Compression::None,
    Compression::Lz4,
    Compression::Snappy,
    Compression::Zstd,
    Compression::Deflate { level: 6 },
];

/// Text that every codec shrinks well past an eighth.
fn compressible() -> Vec<u8> {
    (0..400_u32)
        .flat_map(|at| format!("key-{:05} value of entry {} ", at % 37, at % 11).into_bytes())
        .collect()
}

/// Bytes no codec can shrink.
fn noise(len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[3]
        })
        .collect()
}

#[test]
fn each_codec_stores_a_compressible_block_under_its_own_byte() {
    let payload = compressible();
    let bytes: Vec<Option<u8>> = EVERY
        .iter()
        .map(|codec| compress(*codec, &payload).map(|(_, byte)| byte))
        .collect();
    assert_eq!(bytes, [None, Some(1), Some(2), Some(3), Some(4)]);
    for codec in EVERY.into_iter().skip(1) {
        let (stored, byte) = compress(codec, &payload).expect("compressible");
        assert!(
            stored.len() <= payload.len() - payload.len() / 8,
            "{codec:?}"
        );
        assert_eq!(decompress(byte, &stored).as_deref(), Ok(payload.as_slice()));
    }
}

#[test]
fn a_block_that_does_not_shrink_by_an_eighth_is_stored_as_it_is() {
    let payload = noise(4096);
    for codec in EVERY {
        assert_eq!(compress(codec, &payload), None, "{codec:?}");
    }
    assert_eq!(
        compress(Compression::Deflate { level: 0 }, &compressible()),
        None
    );
}

#[test]
fn deflate_levels_above_ten_compress_as_ten() {
    let payload = compressible();
    assert_eq!(
        compress(Compression::Deflate { level: 200 }, &payload),
        compress(Compression::Deflate { level: 10 }, &payload)
    );
}

#[test]
fn a_cut_short_body_or_a_wrong_length_is_refused_by_every_codec() {
    let payload = compressible();
    for codec in EVERY.into_iter().skip(1) {
        let (stored, byte) = compress(codec, &payload).expect("compressible");
        let cut = &stored[..stored.len() - 7];
        assert!(decompress(byte, cut).is_err(), "{codec:?} cut short");
        let (_, used) = varint::get_u32(&stored).expect("length");
        for claimed in [payload.len() - 1, payload.len() + 1] {
            let mut relabelled = Vec::new();
            varint::put_u32(&mut relabelled, u32::try_from(claimed).expect("fits"));
            relabelled.extend_from_slice(&stored[used..]);
            assert!(
                decompress(byte, &relabelled).is_err(),
                "{codec:?} accepted {claimed} for {}",
                payload.len()
            );
        }
    }
}

#[test]
fn a_length_no_body_could_expand_to_is_refused() {
    for codec in EVERY.into_iter().skip(1) {
        let (stored, byte) = compress(codec, &compressible()).expect("compressible");
        let (_, used) = varint::get_u32(&stored).expect("length");
        let mut bomb = Vec::new();
        varint::put_u32(&mut bomb, u32::MAX);
        bomb.extend_from_slice(&stored[used..]);
        assert!(decompress(byte, &bomb).is_err(), "{codec:?}");
    }
}

#[test]
fn an_unknown_codec_byte_is_refused() {
    assert_eq!(decompress(5, b"\x03abc"), Err("unknown compression"));
    assert_eq!(decompress(0, b"plain").as_deref(), Ok(&b"plain"[..]));
}

proptest! {
    #[test]
    fn whatever_a_codec_stores_reads_back_as_written(
        payload in prop::collection::vec(prop_oneof![Just(b'a'), Just(b'b'), any::<u8>()], 0..3000),
        which in 0..EVERY.len(),
    ) {
        let codec = EVERY[which];
        if let Some((stored, byte)) = compress(codec, &payload) {
            prop_assert!(stored.len() <= payload.len() - payload.len() / 8);
            prop_assert_eq!(decompress(byte, &stored), Ok(payload));
        }
    }
}
