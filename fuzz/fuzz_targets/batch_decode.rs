//! A write batch of arbitrary bytes: decoding and iterating it ends in an error or entries.
#![no_main]

use libfuzzer_sys::fuzz_target;
use tessarikv_format::WriteBatch;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(batch) = WriteBatch::from_bytes(bytes.to_vec()) {
        for entry in batch.iter() {
            let _ = entry;
        }
    }
});
