//! `tessarikv-inspect`: reads a TessariKV database or table file and prints what it holds.

// Production code is held to the workspace lints; a test states its own expectations.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod error;
mod manifest;
mod scan;
mod table;
mod text;

use std::path::Path;
use std::process::ExitCode;

use error::{InspectError, Result};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut out = std::io::stdout().lock();
    match run(&args, &mut out) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tessarikv-inspect: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Runs the command `args` name, writing its output to `out`.
fn run(args: &[String], out: &mut dyn std::io::Write) -> Result<()> {
    let [command, path] = args else {
        return Err(InspectError::Usage(
            "expected a command and a path".to_owned(),
        ));
    };
    let path = Path::new(path);
    match command.as_str() {
        "manifest" => manifest::read_manifest(path)?.print(out),
        "table" => table::print_table(path, out),
        "scan" => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(scan::print_scan(path, out)),
        other => Err(InspectError::Usage(format!("unknown command `{other}`"))),
    }
}

#[cfg(test)]
mod tests {
    use super::run;
    use crate::error::InspectError;

    #[test]
    fn a_command_line_it_does_not_take_is_refused_with_the_usage() {
        for args in [vec![], vec!["dump".to_owned(), "/tmp".to_owned()]] {
            let refused = run(&args, &mut Vec::new());
            assert!(matches!(refused, Err(InspectError::Usage(_))), "{args:?}");
        }
    }
}
