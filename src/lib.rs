#![deny(missing_docs)]
//! A networked key-value database library.
//!
//! The library provides building blocks that can be combined together
//! to build a fully-fledged networked key-value store able to store string
//! keys and values.
//!
//! The key-value database implementation utilizes a log-structured store.
pub mod engine;

use std::{fmt::Display, net::TcpStream};

// TODO: This needs to be split; Engine errors are different from the network
//       bits.
pub use engine::Result;

// TODO: Network Protocol, KvClient, KvServer

use serde::Serialize;
use tracing::{info, instrument};

/// Implements the core functionality of a Key-Value Server
pub struct KvServer {}

impl KvServer {
    /// Create a key-value server
    pub fn new() -> Self {
        Self {}
    }

    /// Handle an incoming client connection
    //TODO: The client field is a bit sketchy. I can probably do this within the
    // function body and actually handle the error; just create a new
    // info_span... Keeping this here since i'm still not sure how to structure
    // this
    #[instrument(level = "info", skip_all, fields(client = stream.peer_addr().unwrap().to_string()))]
    pub fn handle_connection(&mut self, stream: TcpStream) -> Result<()> {
        info!(target: "connection", "accepted connection");
        Ok(())
    }
}

/// List of supported storage engines
#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineType {
    /// Kvs specific storage engine
    #[default]
    Kvs,
    /// Sled storage engine; https://github.com/spacejam/sled
    Sled,
}

impl Display for EngineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineType::Kvs => write!(f, "kvs"),
            EngineType::Sled => write!(f, "sled"),
        }
    }
}
