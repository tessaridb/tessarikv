//! Writers let back in before a sync made one at a time starts.
//!
//! Where syncs run one at a time on the log task, the writers a sync releases come back only
//! after the entries already queued have been taken, so the next sync would start without them
//! and each sync would carry one write. After such a sync ends, the next one waits until every
//! writer known at that moment — those it released and those already queued — has handed in an
//! entry, or until a short deadline, whichever comes first. A lone writer meets the count with
//! its own next entry, so it never waits; a released writer that does not return holds the next
//! sync for at most the deadline. The wait polls the queue between yields and uses no timer, so
//! its bound is not rounded up to a timer's resolution.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{self, error::TryRecvError};

use super::Entry;

/// The writers the last sync made one at a time is waiting for.
pub(super) struct Gather {
    /// How long after a sync ends the next may wait.
    window: Duration,
    round: Option<Round>,
}

struct Round {
    deadline: Instant,
    /// Entries expected since the sync ended: the writers it released and those queued.
    expected: usize,
    /// Entries taken since the sync ended.
    arrived: usize,
}

impl Gather {
    pub(super) fn new(window: Duration) -> Self {
        Self {
            window,
            round: None,
        }
    }

    /// A sync made one at a time has just ended, releasing `released` writers, with `queued`
    /// entries already taken from the queue after it.
    pub(super) fn ended(&mut self, released: usize, queued: usize) {
        self.round = Some(Round {
            deadline: Instant::now()
                .checked_add(self.window)
                .unwrap_or_else(Instant::now),
            expected: released.saturating_add(queued),
            arrived: queued,
        });
    }

    /// `count` entries were taken from the queue.
    pub(super) fn arrived(&mut self, count: usize) {
        if let Some(round) = self.round.as_mut() {
            round.arrived = round.arrived.saturating_add(count);
        }
    }

    /// Takes entries into `pending` until every writer expected since the last sync is back,
    /// the deadline passes or the queue closes; then the round is over. Returns at once when no
    /// round is open.
    pub(super) async fn wait(
        &mut self,
        entries: &mut mpsc::Receiver<Entry>,
        pending: &mut BTreeMap<u64, Entry>,
    ) {
        let Some(mut round) = self.round.take() else {
            return;
        };
        while round.arrived < round.expected && Instant::now() < round.deadline {
            match entries.try_recv() {
                Ok(entry) => {
                    pending.insert(entry.first, entry);
                    round.arrived = round.arrived.saturating_add(1);
                }
                Err(TryRecvError::Empty) => tokio::task::yield_now().await,
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}
