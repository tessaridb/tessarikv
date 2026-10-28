//! The options an open runs with that differ from the previous open's record.

use std::collections::HashMap;

use crate::Db;
use crate::db::open::Access;

/// One option whose recorded value differs from the one the previous open recorded.
///
/// `option` names its place in the record: field names joined by `.`, and `[i]` for the i-th
/// element of a list or tuple, so `keyspaces[0][1].memtable_bytes` is the memtable size of the
/// first named keyspace. Values are the record's text, compared as text and never parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptionChange {
    /// Where the option sits in the record.
    pub option: String,
    /// Its value at the previous open; `None` when that open had no such option.
    pub previous: Option<String>,
    /// Its value now; `None` when this open has no such option.
    pub current: Option<String>,
}

impl Db {
    /// The options this open runs with that differ from what the previous open recorded, in
    /// record order. Empty for a first open, an unchanged one, a database in memory or one
    /// opened read-only, and when the previous record could not be read: it is a report for
    /// people, and nothing the engine does depends on it.
    #[must_use]
    pub fn changed_options(&self) -> &[OptionChange] {
        match &self.access {
            Access::Exclusive { changed, .. } => changed,
            Access::Memory | Access::ReadOnly(_) => &[],
        }
    }
}

/// The options in `current` that differ from those in `previous`, both records' text: first
/// those `current` holds, in its order, then those only `previous` held.
pub(super) fn between(previous: &str, current: &str) -> Vec<OptionChange> {
    let before = flatten(previous);
    let after = flatten(current);
    let earlier: HashMap<&str, &str> = before
        .iter()
        .map(|(option, value)| (option.as_str(), value.as_str()))
        .collect();
    let later: HashMap<&str, &str> = after
        .iter()
        .map(|(option, value)| (option.as_str(), value.as_str()))
        .collect();
    let changed = after.iter().filter_map(|(option, value)| {
        let previous = earlier.get(option.as_str()).copied();
        (previous != Some(value.as_str())).then(|| OptionChange {
            option: option.clone(),
            previous: previous.map(str::to_owned),
            current: Some(value.clone()),
        })
    });
    let removed = before
        .iter()
        .filter(|(option, _)| !later.contains_key(option.as_str()))
        .map(|(option, value)| OptionChange {
            option: option.clone(),
            previous: Some(value.clone()),
            current: None,
        });
    changed.chain(removed).collect()
}

/// Each value in a record as `(place, text)`, read from the layout alone: a `name: value` line
/// is a value, a line ending in `{`, `[` or `(` opens a level named by its field or, unnamed,
/// by its position, and a line starting with `}`, `]` or `)` closes it.
fn flatten(text: &str) -> Vec<(String, String)> {
    // The place of each open level and how many unnamed elements it has seen.
    let mut levels: Vec<(String, usize)> = vec![(String::new(), 0)];
    let mut values = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with(['}', ']', ')']) {
            if levels.len() > 1 {
                levels.pop();
            }
            continue;
        }
        let Some((parent, unnamed)) = levels.last_mut() else {
            break;
        };
        let (place, value) = match line.split_once(": ") {
            Some((name, value))
                if !name.is_empty()
                    && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') =>
            {
                let place = if parent.is_empty() {
                    name.to_owned()
                } else {
                    format!("{parent}.{name}")
                };
                (place, value)
            }
            // The record's outermost value: its fields are named from the top.
            _ if parent.is_empty() => (String::new(), line),
            _ => {
                let place = format!("{parent}[{unnamed}]");
                *unnamed = unnamed.saturating_add(1);
                (place, line)
            }
        };
        let value = value.strip_suffix(',').unwrap_or(value);
        if value.ends_with(['{', '[', '(']) {
            levels.push((place, 0));
        } else {
            values.push((place, value.to_owned()));
        }
    }
    values
}
