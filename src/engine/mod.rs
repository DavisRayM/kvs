//! Supported storage engines
//!
//! Storage engines handle how data is stored, read and represented on disk.

use tracing::subscriber::SetGlobalDefaultError;
pub mod kvs;

pub use kvs::KvStore;

/// Custom `Result` type that represents a success or error of KvStore
/// functionality
pub type Result<T> = std::result::Result<T, StoreError>;

/// Key-Value storage engine trait.
///
/// Defines the interface used to interact with storage engines
pub trait KvEngine {
    /// Set the value of a key.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the value of a key.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given key.
    ///
    /// # Errors
    ///
    /// An error is returned if the key does not exist.
    fn remove(&mut self, key: String) -> Result<()>;
}

/// The error type for StorageEngine operations.
#[derive(Debug)]
pub enum StoreError {
    /// An IO Error occurred while accessing the underlying file.
    Io(std::io::Error),
    /// A serde error occurred while serializing or deserializing a log entry.
    Serde(serde_json::error::Error),
    /// An operation failed due to a missing key. Often occurs when
    /// trying to remove a key that does not exist
    NotFound,
    /// An error occurred while accessing a log fragment
    Fragment(String),

    // TODO: Everything from this point needs to move; It's not related to the storage engines
    /// An error occurred while setting default tracing subscriber
    SubscriberGlobalDefault(SetGlobalDefaultError),
    /// An error occurred during address parsing
    AddrParse(std::net::AddrParseError),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Io(err) => write!(f, "IO error: {}", err),
            StoreError::NotFound => write!(f, "Key not found"),
            StoreError::Serde(err) => write!(f, "Serde error: {}", err),
            StoreError::Fragment(desc) => write!(f, "Fragment error: {}", desc),
            StoreError::SubscriberGlobalDefault(err) => {
                write!(f, "Tracing subscriber error: {}", err)
            }
            StoreError::AddrParse(err) => write!(f, "Address parsing error: {}", err),
        }
    }
}

impl std::error::Error for StoreError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match self {
            StoreError::Io(err) => Some(err),
            StoreError::NotFound => None,
            StoreError::Serde(err) => Some(err),
            StoreError::Fragment(_) => None,
            StoreError::SubscriberGlobalDefault(err) => Some(err),
            StoreError::AddrParse(err) => Some(err),
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::error::Error> for StoreError {
    fn from(err: serde_json::error::Error) -> Self {
        Self::Serde(err)
    }
}

impl From<SetGlobalDefaultError> for StoreError {
    fn from(err: SetGlobalDefaultError) -> Self {
        Self::SubscriberGlobalDefault(err)
    }
}

impl From<std::net::AddrParseError> for StoreError {
    fn from(err: std::net::AddrParseError) -> Self {
        Self::AddrParse(err)
    }
}
