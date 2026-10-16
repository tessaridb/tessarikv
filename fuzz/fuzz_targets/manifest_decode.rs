//! A manifest edit of arbitrary bytes decodes to an edit or an error.
#![no_main]

use libfuzzer_sys::fuzz_target;
use tessarikv_format::ManifestEdit;

fuzz_target!(|bytes: &[u8]| {
    let _ = ManifestEdit::decode(bytes);
});
