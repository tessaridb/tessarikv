//! A seal header of arbitrary bytes decodes to a header or an error.
#![no_main]

use libfuzzer_sys::fuzz_target;
use tessarikv_format::SealHeader;

fuzz_target!(|bytes: &[u8]| {
    let _ = SealHeader::decode(bytes);
});
