use std::{io::Write, net::TcpStream};

use clap::{Parser, Subcommand};
use kvs::Result;

#[derive(Parser)]
#[command(name = env!("CARGO_BIN_NAME"), version = env!("CARGO_PKG_VERSION"), about = env!("CARGO_PKG_DESCRIPTION"), long_about = None)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,
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

    let mut stream = TcpStream::connect(args.addr)?;

    stream.write_all(&[1])?;
    Ok(())
}
