use std::{
    io,
    net::{SocketAddr, TcpListener},
    str::FromStr,
};

use clap::Parser;
use kvs::{EngineType, KvServer, Result};
use tracing::{event, Level};

#[derive(Parser)]
#[command(name = env!("CARGO_BIN_NAME"), version = env!("CARGO_PKG_VERSION"), about = env!("CARGO_PKG_DESCRIPTION"), long_about = None)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,
    #[arg(long, default_value = "kvs")]
    engine: EngineType,
}

fn main() -> Result<()> {
    let subscriber = tracing_subscriber::fmt().with_writer(io::stderr).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Cli::parse();
    event!(
        name: "startup",
        target: "startup",
        Level::INFO,
        version = env!("CARGO_PKG_VERSION"),
        address = args.addr,
        engine = args.engine.to_string(),
    );

    let address = SocketAddr::from_str(&args.addr)?;
    let listener = TcpListener::bind(address)?;
    let mut server = KvServer::new();

    // NOTE: Can't push this to CI; Unless you like long-running tests
    // for stream in listener.incoming() {
    //     server.handle_connection(stream?)?;
    // }

    Ok(())
}
