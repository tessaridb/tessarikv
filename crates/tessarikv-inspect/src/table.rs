//! A table file's entries, in order: key, sequence, kind and value.

use std::io::Write;
use std::path::Path;

use tessarikv_format::Table;
use tessarikv_types::{InternalKeyRef, compare_internal};

use crate::error::{InspectError, Result, read};
use crate::manifest::user_key;
use crate::text::escaped;

/// Writes every entry of the table file at `path` to `out`, then its range deletes' count, its
/// entry count and the blob files it refers to. Encrypted tables are not read.
///
/// # Errors
///
/// [`InspectError::Read`] when the file cannot be read, [`InspectError::Unreadable`] when it is
/// not a table this build reads, and [`InspectError::Output`] when writing fails.
pub fn print_table(path: &Path, out: &mut dyn Write) -> Result<()> {
    let unreadable = |detail: String| InspectError::Unreadable {
        path: path.to_path_buf(),
        what: "table",
        detail,
    };
    let bytes = read(path)?;
    let table = Table::open(bytes, compare_internal, user_key)
        .map_err(|error| unreadable(format!("{error} (an encrypted table is not read)")))?;
    let mut entries = table.iter();
    entries.seek_to_first();
    while entries.valid() {
        let key = InternalKeyRef::parse(entries.key())
            .map_err(|_| unreadable("a stored key has no trailer".to_owned()))?;
        let kind = key
            .trailer()
            .kind()
            .map_or_else(|_| "unknown".to_owned(), |kind| format!("{kind:?}"));
        writeln!(
            out,
            "{} @{} {} {}",
            escaped(key.user_key()),
            key.trailer().sequence().get(),
            kind,
            escaped(entries.value()),
        )?;
        entries.next();
    }
    entries
        .status()
        .map_err(|error| unreadable(error.to_string()))?;
    writeln!(out, "entries {}", table.entries())?;
    let blobs: Vec<String> = table.blob_files().iter().map(u64::to_string).collect();
    writeln!(out, "blob files [{}]", blobs.join(", "))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::print_table;
    use crate::error::InspectError;
    use crate::manifest::tests::written;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_table_lists_its_entries_in_order() {
        let dir = tempfile::tempdir().expect("a directory");
        written(dir.path()).await;
        let table = std::fs::read_dir(dir.path())
            .expect("listed")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
            .expect("a table file");
        let mut out = Vec::new();
        print_table(&table, &mut out).expect("printed");
        let text = String::from_utf8(out).expect("utf-8");
        assert!(text.starts_with("apple @1 Value red\n"), "{text}");
        assert!(text.contains("entries 1\n"), "{text}");
    }

    #[test]
    fn bytes_that_are_no_table_are_refused_by_name() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("1.sst");
        std::fs::write(&path, b"not a table").expect("written");
        assert!(matches!(
            print_table(&path, &mut Vec::new()),
            Err(InspectError::Unreadable { what: "table", .. })
        ));
    }
}
