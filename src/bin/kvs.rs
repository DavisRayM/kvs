use std::process::exit;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = env!("CARGO_BIN_NAME"), version = env!("CARGO_PKG_VERSION"), about = env!("CARGO_PKG_DESCRIPTION"), long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Get the value for a key.
    Get { key: String },
    /// Remove given key from store, if it exists.
    Rm { key: String },
    /// Set a key to value.
    Set { key: String, value: String },
}

fn main() {
    let args = Cli::parse();

    match &args.command {
        Command::Get { .. } => {
            eprintln!("unimplemented");
            exit(1)
        }
        Command::Rm { .. } => {
            eprintln!("unimplemented");
            exit(1)
        }
        Command::Set { .. } => {
            eprintln!("unimplemented");
            exit(1)
        }
    }
}
