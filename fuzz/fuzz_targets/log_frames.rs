//! A log segment of arbitrary bytes: reading frames ends at a clean, torn or corrupt end.
#![no_main]

use libfuzzer_sys::fuzz_target;
use tessarikv_format::{FrameReader, LogRead};

fuzz_target!(|bytes: &[u8]| {
    let mut reader = FrameReader::new(bytes, 1);
    while let LogRead::Frame(_) = reader.next_frame() {}
});
