//! The live keys of a database, read through a read-only open.

use std::io::Write;
use std::path::Path;

use tessarikv::{Db, DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions};

use crate::error::Result;
use crate::manifest::read_manifest;
use crate::text::escaped;

/// Writes every live key and value of every keyspace of the database in `dir` to `out`, as the
/// database reads them, under a read-only open that changes no file. Keys holding merge
/// operands need the keyspace's operator, which this tool does not have: such a read fails.
///
/// # Errors
///
/// The errors of [`read_manifest`] and of [`Db::open_read_only`], a failed read, and
/// [`crate::error::InspectError::Output`] when writing fails.
pub async fn print_scan(dir: &Path, out: &mut dyn Write) -> Result<()> {
    let manifest = read_manifest(dir)?;
    let mut options = DbOptions::builder();
    for keyspace in &manifest.keyspaces {
        options = options.keyspace(keyspace.name.clone(), KeyspaceOptions::default());
    }
    let db = Db::open_read_only(dir, options.build()?).await?;
    let mut keyspaces = vec![(KeyspaceId::DEFAULT, "default".to_owned())];
    keyspaces.extend(
        manifest
            .keyspaces
            .iter()
            .map(|keyspace| (KeyspaceId::new(keyspace.id), keyspace.name.clone())),
    );
    for (id, name) in keyspaces {
        writeln!(out, "keyspace {} {}", id.get(), escaped(name.as_bytes()))?;
        let mut keys = db.iter(id, &ReadOptions::default())?;
        keys.seek_to_first().await?;
        while keys.valid() {
            writeln!(out, "{}\t{}", escaped(keys.key()), escaped(keys.value()))?;
            keys.next().await?;
        }
    }
    db.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::print_scan;
    use crate::manifest::tests::written;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_scan_lists_the_live_keys_of_every_keyspace() {
        let dir = tempfile::tempdir().expect("a directory");
        written(dir.path()).await;
        let before = std::fs::read_dir(dir.path()).expect("listed").count();
        let mut out = Vec::new();
        print_scan(dir.path(), &mut out).await.expect("printed");
        let text = String::from_utf8(out).expect("utf-8");
        assert_eq!(
            text,
            "keyspace 0 default\napple\tred\npear\tgreen\nkeyspace 1 fruit\n"
        );
        assert_eq!(
            std::fs::read_dir(dir.path()).expect("listed").count(),
            before
        );
    }
}
