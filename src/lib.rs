#![deny(missing_docs)]
//! A networked key-value database library.
//!
//! The library provides building blocks that can be combined together
//! to build a fully-fledged networked key-value store able to store string
//! keys and values.
//!
//! The key-value database implementation utilizes a log-structured store.
pub mod engine;

use std::fmt::Display;

pub use engine::Result;

use serde::Serialize;

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
