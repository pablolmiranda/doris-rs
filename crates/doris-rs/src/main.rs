//! `doris-rs <role> [options]`. M2 has one role: `be --config <be.conf>`.

use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "usage: doris-rs be --config <be.conf>";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [role, flag, path] if role == "be" && flag == "--config" => run_be(PathBuf::from(path)),
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn run_be(path: PathBuf) -> ExitCode {
    let cfg = match doris_be::config::BeConfig::load(&path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("doris-rs be: {e}");
            return ExitCode::from(1);
        }
    };
    match doris_be::run(cfg) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("doris-rs be: {e}");
            ExitCode::from(1)
        }
    }
}
