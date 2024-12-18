//! Supported storage engines
//!
//! Storage engines handle how data is stored, read and represented on disk.

use tracing::subscriber::SetGlobalDefaultError;
pub mod kvs;

/// Custom `Result` type that represents a success or error of KvStore
/// functionality
pub type Result<T> = std::result::Result<T, StoreError>;

/// The error type for StorageEngine operations.
#[derive(Debug)]
pub enum StoreError {
    /// An IO Error occured while accessing the underlying file.
    Io(std::io::Error),
    /// A serde error occured while serializing or deserializing a log entry.
    Serde(serde_json::error::Error),
    /// An operation failed due to a missing key. Often occurs when
    /// trying to remove a key that does not exist
    NotFound,
    /// An error occurred while accessing a log fragment
    Fragment(String),
    /// An error occurred while setting default tracing subscriber
    SubscriberGlobalDefault(SetGlobalDefaultError),
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
    fn from(value: SetGlobalDefaultError) -> Self {
        Self::SubscriberGlobalDefault(value)
    }
}
