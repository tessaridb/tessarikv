use proptest::prelude::*;

use super::Compression;
use super::codec::{compress, decompress, max_payload};
use super::handle::write_block;
use super::unseal::{unseal, unseal_owned};
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
fn every_codec_refuses_a_block_larger_than_a_block_can_be() {
    for codec in EVERY.into_iter().skip(1) {
        let (stored, byte) = compress(codec, &compressible()).expect("compressible");
        let (_, used) = varint::get_u32(&stored).expect("length");
        let mut claim = Vec::new();
        let too_large = u32::try_from(max_payload() + 1).expect("fits the length field");
        varint::put_u32(&mut claim, too_large);
        claim.extend_from_slice(&stored[used..]);
        assert_eq!(
            decompress(byte, &claim),
            Err("claims more than a block can hold"),
            "{codec:?}"
        );
    }
}

/// A valid Zstandard frame of run-length blocks that expands to `len` bytes of `byte`.
fn zstd_runs(len: usize, byte: u8) -> Vec<u8> {
    const RUN: usize = 128 * 1024;
    // Magic, a descriptor without a content size, and a 128 KiB window.
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38];
    let mut left = len;
    while left > 0 {
        let size = left.min(RUN);
        left -= size;
        let last = u32::from(left == 0);
        let header = last | (1 << 1) | (u32::try_from(size).expect("a run") << 3);
        frame.extend_from_slice(&header.to_le_bytes()[..3]);
        frame.push(byte);
    }
    frame
}

#[test]
fn a_zstd_frame_that_expands_past_any_block_is_refused_before_it_is_decoded() {
    let len = max_payload() + 1;
    let mut block = Vec::new();
    varint::put_u32(
        &mut block,
        u32::try_from(len).expect("fits the length field"),
    );
    block.extend_from_slice(&zstd_runs(len, 7));
    assert_eq!(
        decompress(3, &block).map(|payload| payload.len()),
        Err("claims more than a block can hold")
    );
}

#[test]
fn a_block_holding_one_entry_past_the_target_size_still_reads() {
    // 32 MiB of one byte: more than the largest target size, as a block holding one large value is.
    let payload = vec![b'v'; 32 * 1024 * 1024];
    for codec in EVERY.into_iter().skip(1) {
        let (stored, byte) = compress(codec, &payload).expect("compressible");
        assert_eq!(
            decompress(byte, &stored).as_deref(),
            Ok(payload.as_slice()),
            "{codec:?}"
        );
    }
}

#[test]
fn an_owned_block_unseals_as_the_borrowed_one_for_every_codec() {
    for payload in [compressible(), noise(4096)] {
        for codec in EVERY {
            let mut sealed = Vec::new();
            write_block(&mut sealed, 0, &payload, codec, None).expect("written");
            let borrowed = unseal(&sealed, 0, None).expect("borrowed");
            assert_eq!(borrowed, payload, "{codec:?}");
            assert_eq!(
                unseal_owned(sealed.clone(), 0, None),
                Ok(borrowed),
                "{codec:?}"
            );
            let last = sealed.len() - 1;
            sealed[last] ^= 1;
            assert!(unseal_owned(sealed, 0, None).is_err(), "{codec:?}");
        }
    }
}

#[test]
fn an_uncompressed_owned_block_becomes_its_payload_without_a_copy() {
    let payload = noise(4096);
    let mut sealed = Vec::new();
    write_block(&mut sealed, 0, &payload, Compression::Lz4, None).expect("written");
    let buffer = sealed.as_ptr();
    let unsealed = unseal_owned(sealed, 0, None).expect("unsealed");
    assert_eq!(unsealed, payload);
    assert_eq!(unsealed.as_ptr(), buffer, "the read buffer is the payload");
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
