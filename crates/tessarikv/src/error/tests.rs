use std::error::Error as _;
use std::path::PathBuf;

use super::*;

fn samples() -> Vec<Error> {
    vec![
        Error::Io {
            operation: "write",
            path: PathBuf::from("db/000001.log"),
            source: std::io::Error::other("disk"),
        },
        Error::NotFound {
            path: PathBuf::from("db/CURRENT"),
        },
        Error::Locked {
            path: PathBuf::from("db/LOCK"),
        },
        Error::InvalidOptions {
            option: "keyspace.block_bytes",
            reason: "must be at least 256".to_owned(),
        },
        Error::Corruption {
            what: "block",
            path: Some(PathBuf::from("db/000007.tkv")),
            offset: Some(4096),
            detail: "checksum mismatch".to_owned(),
        },
        Error::ShortRead {
            path: PathBuf::from("db/000007.tkv"),
            requested_end: 10,
            available: 5,
        },
        Error::Memtable {
            reason: "internal arena error",
        },
        Error::UnknownKeyspace { keyspace: 9 },
        Error::Unsupported { what: "x" },
        Error::SequenceExhausted,
        Error::LogFailed,
        Error::FlushFailed,
        Error::CompactionFailed,
        Error::ReadFailed,
        Error::WriteStalled { reason: "x" },
        Error::AlreadyExists {
            path: PathBuf::from("db"),
        },
        Error::Closed,
        Error::Conflict,
        Error::UnsupportedFormat {
            path: PathBuf::from("db/MANIFEST-00000000000000000001"),
            found: 2,
            newest: 1,
        },
        Error::ReadOnly { what: "a write" },
    ]
}

/// Each variant lands in exactly the category the error model assigns it.
#[test]
fn every_variant_maps_to_its_category() {
    let categories: Vec<_> = samples().iter().map(Error::category).collect();
    assert_eq!(
        categories,
        [
            ErrorCategory::Io,
            ErrorCategory::NotFound,
            ErrorCategory::Unavailable,
            ErrorCategory::Validation,
            ErrorCategory::Corruption,
            ErrorCategory::Corruption,
            ErrorCategory::Internal,
            ErrorCategory::Validation,
            ErrorCategory::Validation,
            ErrorCategory::Internal,
            ErrorCategory::Io,
            ErrorCategory::Io,
            ErrorCategory::Io,
            ErrorCategory::Internal,
            ErrorCategory::Unavailable,
            ErrorCategory::Conflict,
            ErrorCategory::ShutDown,
            ErrorCategory::Conflict,
            ErrorCategory::Validation,
            ErrorCategory::ReadOnly,
        ]
    );
}

/// Codes are API: this list may grow, never change.
#[test]
fn codes_are_stable_strings() {
    let codes: Vec<_> = samples().iter().map(Error::code).collect();
    assert_eq!(
        codes,
        [
            "io",
            "not_found",
            "locked",
            "invalid_options",
            "corruption",
            "short_read",
            "memtable",
            "unknown_keyspace",
            "unsupported",
            "sequence_exhausted",
            "log_failed",
            "flush_failed",
            "compaction_failed",
            "read_failed",
            "write_stalled",
            "already_exists",
            "closed",
            "conflict",
            "unsupported_format",
            "read_only"
        ]
    );
}

#[test]
fn only_transient_failures_are_retryable() {
    let retryable: Vec<_> = samples().iter().map(Error::is_retryable).collect();
    assert_eq!(
        retryable,
        [
            false, false, true, false, false, false, false, false, false, false, false, false,
            false, false, true, false, false, true, false, false
        ]
    );
    let interrupted = Error::Io {
        operation: "write",
        path: PathBuf::from("x"),
        source: std::io::Error::from(std::io::ErrorKind::Interrupted),
    };
    assert!(interrupted.is_retryable());
}

#[test]
fn the_io_cause_is_kept_as_the_source() {
    let error = &samples()[0];
    assert_eq!(
        error.source().map(ToString::to_string),
        Some("disk".to_owned())
    );
}

#[test]
fn a_missing_file_from_the_os_becomes_not_found() {
    let error = Error::io(
        "open",
        std::path::Path::new("x"),
        std::io::Error::from(std::io::ErrorKind::NotFound),
    );
    assert_eq!(error.code(), "not_found");
}
