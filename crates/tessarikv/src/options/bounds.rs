//! The one range check every option goes through.

use crate::{Error, Result};

/// Refuses `value` unless `min <= value <= max`, naming the option by its field path.
pub(super) fn check_range(option: &'static str, value: u64, min: u64, max: u64) -> Result<()> {
    if (min..=max).contains(&value) {
        return Ok(());
    }
    Err(Error::InvalidOptions {
        option,
        reason: format!("{value} is outside {min}..={max}"),
    })
}
