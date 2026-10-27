//! An iterator on a key whose newest visible version is a merge operand.

use super::DbIterator;
use crate::Result;
use crate::db::read::operands::merged_in_walk;

impl DbIterator<'_> {
    /// Reads the current key's value by gathering its operands, keeps it in `saved`, and seeks
    /// back onto the key so the next move passes over its versions.
    pub(super) async fn merge_here(&mut self) -> Result<()> {
        let (keyspace, operator) = &self.merging;
        let value = merged_in_walk(
            &mut self.versions,
            operator.as_ref(),
            *keyspace,
            self.snapshot,
            &self.fragments,
        )
        .await;
        let value = match value {
            Ok(value) => value,
            Err(error) => {
                self.key.clear();
                return Err(error);
            }
        };
        self.saved = value;
        self.merged = true;
        self.versions.seek(&self.key, self.snapshot).await;
        Ok(())
    }
}
