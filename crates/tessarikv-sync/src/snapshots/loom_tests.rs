use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};

use super::SnapshotRegistry;

/// A snapshot taken while a compaction reads its horizon is either seen by that compaction or
/// is at or above the compaction's visible sequence: never older and unseen.
#[test]
fn a_registration_is_never_missed_by_a_concurrent_horizon() {
    loom::model(|| {
        let registry = Arc::new(SnapshotRegistry::new());
        let visible = Arc::new(AtomicU64::new(1));
        let (writer_visible, compactor_visible) = (Arc::clone(&visible), Arc::clone(&visible));
        let compactor_registry = Arc::clone(&registry);
        let writer = loom::thread::spawn(move || writer_visible.store(2, Ordering::Release));
        let compactor = loom::thread::spawn(move || {
            let horizon = compactor_registry.horizon(compactor_visible.load(Ordering::Acquire));
            (horizon.visible(), horizon.snapshots().to_vec())
        });
        let snapshot = registry
            .register(|| visible.load(Ordering::Acquire))
            .unwrap();
        writer.join().unwrap();
        let (seen_visible, seen) = compactor.join().unwrap();
        let sequence = snapshot.sequence();
        assert!(
            seen.contains(&sequence) || sequence >= seen_visible,
            "snapshot {sequence} missed by a horizon at {seen_visible} that saw {seen:?}"
        );
        registry.release(snapshot);
    });
}
