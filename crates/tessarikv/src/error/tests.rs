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
    ]
}

/// Each variant lands in exactly the category the error model (ADR-0005) assigns it.
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
            "short_read"
        ]
    );
}

#[test]
fn only_transient_failures_are_retryable() {
    let retryable: Vec<_> = samples().iter().map(Error::is_retryable).collect();
    assert_eq!(retryable, [false, false, true, false, false, false]);
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
