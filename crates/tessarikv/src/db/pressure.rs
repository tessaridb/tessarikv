//! What the levels ask of writers.
//!
//! After every change to the levels, and when it starts, the installer weighs every keyspace by
//! its own options: its level 0 against its slowdown and stop triggers (a first-in-first-out
//! keyspace keeps every table at level 0 and has none), and the bytes compaction owes it against
//! its soft and hard limits. The worst of all keyspaces is published to writers, who read that
//! one figure per write instead of counting tables (see `FlushSignals::admit`).

use super::compaction;
use super::installer::Flusher;
use super::view::View;
use crate::{CompactionStyle, KeyspaceOptions};

/// What writers must do, from the mildest to the worst.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Pressure {
    /// Write freely.
    None,
    /// Pace writes: compaction owes a keyspace its soft limit's worth of bytes.
    SlowOwed,
    /// Pace writes: a keyspace's level 0 is past its slowdown trigger.
    SlowLevel0,
    /// Hold writes: compaction owes a keyspace its hard limit's worth of bytes.
    StopOwed,
    /// Hold writes: a keyspace's level 0 holds its stop trigger's worth of tables.
    StopLevel0,
}

impl Pressure {
    /// The code it is published as; [`Pressure::from_code`] reads it back.
    pub(super) fn code(self) -> u8 {
        match self {
            Self::None => 0,
            Self::SlowOwed => 1,
            Self::SlowLevel0 => 2,
            Self::StopOwed => 3,
            Self::StopLevel0 => 4,
        }
    }

    /// The pressure published as `code`; none for a code that names none.
    pub(super) fn from_code(code: u8) -> Self {
        match code {
            1 => Self::SlowOwed,
            2 => Self::SlowLevel0,
            3 => Self::StopOwed,
            4 => Self::StopLevel0,
            _ => Self::None,
        }
    }

    /// Why a write is paced or held.
    pub(super) fn reason(self) -> &'static str {
        match self {
            Self::None => "nothing holds writes",
            Self::SlowOwed => "compaction owes a keyspace its soft limit's worth of bytes",
            Self::SlowLevel0 => "level 0 of a keyspace is past its slowdown trigger",
            Self::StopOwed => {
                "compaction owes a keyspace its hard limit's worth of bytes, waiting for a compaction"
            }
            Self::StopLevel0 => {
                "level 0 of a keyspace holds its stop trigger's worth of tables, waiting for a \
                 compaction"
            }
        }
    }
}

/// The pressure the levels put on writers, and the most bytes compaction owes any keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Load {
    pub(super) pressure: Pressure,
    pub(super) owed: u64,
}

/// The load the levels of `view` put on writers, each keyspace of `keyspaces` weighed by its own
/// options; the worst wins.
pub(super) fn of<'a>(
    view: &View,
    keyspaces: impl IntoIterator<Item = (u32, &'a KeyspaceOptions)>,
) -> Load {
    let mut load = Load {
        pressure: Pressure::None,
        owed: 0,
    };
    for (keyspace, options) in keyspaces {
        let owed = compaction::owed(view, keyspace, options);
        let level0 = view.level0_files(keyspace);
        let fifo = matches!(options.compaction(), CompactionStyle::Fifo { .. });
        let reached =
            |trigger: u32| !fifo && level0 >= usize::try_from(trigger).unwrap_or(usize::MAX);
        let pressure = if reached(options.level0_stop_trigger()) {
            Pressure::StopLevel0
        } else if owed >= options.hard_pending_compaction_bytes() {
            Pressure::StopOwed
        } else if reached(options.level0_slowdown_trigger()) {
            Pressure::SlowLevel0
        } else if owed >= options.soft_pending_compaction_bytes() {
            Pressure::SlowOwed
        } else {
            Pressure::None
        };
        load.pressure = load.pressure.max(pressure);
        load.owed = load.owed.max(owed);
    }
    load
}

impl Flusher {
    /// What the current levels ask of writers.
    pub(super) fn load(&self) -> Load {
        of(&self.view.load(), self.all_keyspaces())
    }
}
