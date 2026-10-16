use super::*;
use crate::error::Error;

const LOG: u64 = 7;

/// A test offset as the reader reports it.
fn at(offset: usize) -> u64 {
    u64::try_from(offset).expect("test offsets fit in u64")
}

/// Frame size of a test payload, which is always within the limit.
fn size(payload_len: usize) -> usize {
    frame_bytes(payload_len).expect("test payloads are within the limit")
}

fn framed(payloads: &[&[u8]]) -> Vec<u8> {
    let mut log = Vec::new();
    for payload in payloads {
        let start = log.len();
        log.resize(start + size(payload.len()), 0);
        encode_frame(LOG, payload, &mut log[start..]).expect("destination is exactly one frame");
    }
    log
}

fn read_all(bytes: &[u8], log_number: u64) -> (Vec<Vec<u8>>, LogEnd) {
    let mut reader = FrameReader::new(bytes, log_number);
    let mut payloads = Vec::new();
    loop {
        match reader.next_frame() {
            LogRead::Frame(frame) => payloads.push(frame.to_vec()),
            LogRead::End(end) => return (payloads, end),
        }
    }
}

#[test]
fn frames_round_trip_in_order_and_end_cleanly() {
    let log = framed(&[b"first", b"", b"third payload"]);
    let (payloads, end) = read_all(&log, LOG);
    assert_eq!(
        payloads,
        vec![b"first".to_vec(), Vec::new(), b"third payload".to_vec()]
    );
    assert_eq!(
        end,
        LogEnd {
            offset: at(log.len()),
            kind: EndKind::Clean
        }
    );
}

#[test]
fn a_zeroed_tail_is_a_clean_end_at_the_last_frame() {
    let mut log = framed(&[b"only"]);
    let valid = at(log.len());
    log.resize(log.len() + 4096, 0);
    let (payloads, end) = read_all(&log, LOG);
    assert_eq!(payloads, vec![b"only".to_vec()]);
    assert_eq!(
        end,
        LogEnd {
            offset: valid,
            kind: EndKind::Clean
        }
    );
}

#[test]
fn every_truncation_of_the_last_frame_is_a_torn_tail_after_the_first() {
    let log = framed(&[b"kept", b"cut short"]);
    let first = size(4);
    for cut in first + 1..log.len() {
        let (payloads, end) = read_all(&log[..cut], LOG);
        assert_eq!(payloads, vec![b"kept".to_vec()], "cut at {cut}");
        assert_eq!(
            end,
            LogEnd {
                offset: at(first),
                kind: EndKind::Torn
            },
            "cut at {cut}"
        );
    }
}

#[test]
fn any_flipped_bit_ends_the_log_before_the_damaged_frame() {
    let log = framed(&[b"kept", b"damaged"]);
    let first = size(4);
    for byte in first..log.len() {
        for bit in 0..8 {
            let mut damaged = log.clone();
            damaged[byte] ^= 1 << bit;
            let (payloads, end) = read_all(&damaged, LOG);
            assert_eq!(payloads, vec![b"kept".to_vec()], "byte {byte} bit {bit}");
            assert_eq!(end.offset, at(first), "byte {byte} bit {bit}");
            assert_ne!(end.kind, EndKind::Clean, "byte {byte} bit {bit}");
        }
    }
}

#[test]
fn a_frame_of_another_log_is_foreign_and_ends_the_log() {
    let mut log = framed(&[b"mine"]);
    let mine = at(log.len());
    let start = log.len();
    log.resize(start + size(5), 0);
    encode_frame(LOG + 1, b"stale", &mut log[start..]).expect("destination is exactly one frame");
    let (payloads, end) = read_all(&log, LOG);
    assert_eq!(payloads, vec![b"mine".to_vec()]);
    assert_eq!(
        end,
        LogEnd {
            offset: mine,
            kind: EndKind::Foreign
        }
    );
}

#[test]
fn a_length_field_past_the_limit_is_torn_without_allocating() {
    let mut log = framed(&[b"x"]);
    log[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    let (payloads, end) = read_all(&log, LOG);
    assert!(payloads.is_empty());
    assert_eq!(
        end,
        LogEnd {
            offset: 0,
            kind: EndKind::Torn
        }
    );
}

#[test]
fn encoding_refuses_a_wrong_sized_destination_and_an_oversized_payload() {
    let mut short = vec![0; size(3) - 1];
    assert!(matches!(
        encode_frame(LOG, b"abc", &mut short),
        Err(Error::Malformed { .. })
    ));
    assert!(matches!(
        frame_bytes(MAX_LOG_PAYLOAD_BYTES + 1),
        Err(Error::TooLarge { .. })
    ));
    assert_eq!(size(0), FRAME_HEADER_BYTES);
}

#[test]
fn a_header_stamped_from_the_payload_checksum_matches_a_whole_frame() {
    let payload = b"stamped later by the log writer";
    let mut whole = vec![0; size(payload.len())];
    encode_frame(LOG, payload, &mut whole).expect("destination is exactly one frame");
    let header = frame_header(LOG, payload.len(), payload_checksum(payload))
        .expect("test payloads are within the limit");
    assert_eq!(&whole[..FRAME_HEADER_BYTES], &header[..]);
}

#[test]
fn a_payload_checksum_can_be_taken_in_parts() {
    let (front, back) = (b"first sequence", b"then the batch");
    let whole = [&front[..], &back[..]].concat();
    assert_eq!(
        concat_checksum(payload_checksum(front), payload_checksum(back), back.len()),
        payload_checksum(&whole)
    );
}

#[test]
fn the_checksum_covers_the_payload_then_the_length_and_the_log_number() {
    let payload = b"payload first, so the log writer appends twelve bytes";
    let mut frame = vec![0; size(payload.len())];
    encode_frame(LOG, payload, &mut frame).expect("destination is exactly one frame");
    let fields = &frame[4..FRAME_HEADER_BYTES];
    let expected = crc32c::crc32c_append(crc32c::crc32c(payload), fields);
    assert_eq!(frame[..4], expected.to_le_bytes());
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(512))]

    /// Whatever the bytes, reading frames ends, and never panics: each frame read consumes bytes.
    #[test]
    fn arbitrary_bytes_end_and_never_panic(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512)) {
        let mut reader = FrameReader::new(&bytes, 1);
        let mut frames = 0_usize;
        loop {
            match reader.next_frame() {
                LogRead::Frame(_) => {
                    frames += 1;
                    proptest::prop_assert!(frames <= bytes.len(), "more frames than bytes");
                }
                LogRead::End(end) => {
                    proptest::prop_assert!(end.offset <= bytes.len() as u64);
                    break;
                }
            }
        }
    }
}
