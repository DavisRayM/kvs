use std::process::exit;

use clap::{Parser, Subcommand};
use kvs::{KvStore, Result};

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

fn main() -> Result<()> {
    let args = Cli::parse();
    let path = std::env::current_dir()?;
    let mut store = KvStore::open(path)?;

    match &args.command {
        Command::Get { key } => match store.get(key.to_owned())? {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        },
        Command::Rm { key } => {
            if let Err(err) = store.remove(key.to_owned()) {
                println!("{}", err);
                exit(2);
            }
        }
        Command::Set { key, value } => store.set(key.to_owned(), value.to_owned())?,
    }

    Ok(())
}
