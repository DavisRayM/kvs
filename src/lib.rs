#![deny(missing_docs)]
//! A networked key-value database library.
//!
//! The library provides building blocks that can be combined together
//! to build a fully-fledged networked key-value store able to store string
//! keys and values.
//!
//! The key-value database implementation utilizes a log-structured store.

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    path::PathBuf,
};

/// File extension for logs
pub static LOG_EXTENSION: &str = ".kv";

/// Custom `Result` type that represents a success or error of KvStore
/// functionality
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The error type for KvStore operations.
#[derive(Debug)]
pub enum StoreError {
    /// An IO Error occured while accessing the underlying file.
    Io(std::io::Error),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Io(err) => write!(f, "IO Error: {}", err),
        }
    }
}

impl std::error::Error for StoreError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match self {
            StoreError::Io(err) => Some(err),
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// A list specifying supported Write-Ahead Log(WAL) entries.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum LogEntry {
    Set { key: String, value: String },
    Rm { key: String },
}

/// Represents a key-value store.
pub struct KvStore {
    index: HashMap<String, String>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl KvStore {
    /// Opens a key-value store at the given directory path.
    ///
    ///
    /// If Key-Value store exists at the path, the pre-existing stores index is
    /// loaded into memory and subsequent changes are stored.
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<Self> {
        let dir_path: PathBuf = dir_path.into();
        let path = dir_path.join(format!("1.{}", LOG_EXTENSION));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file);

        Ok(Self {
            index: HashMap::new(),
            reader,
            writer,
        })
    }

    /// Set value for a key. Overrides stored value if any.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry = LogEntry::Set {
            key: key.clone(),
            value,
        };

        let _ = self.writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.writer, &entry)?;
        Ok(())
    }

    /// Get the value of a key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.reader.seek(SeekFrom::Start(0))?;

        while let Ok(entry) = serde_json::from_reader::<_, LogEntry>(&mut self.reader) {
            match entry {
                LogEntry::Set { key, value } => self.index.insert(key, value),
                LogEntry::Rm { ref key } => self.index.remove(key),
            };
        }

        Ok(self.index.get(&key).cloned())
    }

    /// Remove the value of a key from the store, If it exists.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let entry = LogEntry::Rm { key: key.clone() };

        self.writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.writer, &entry)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::TempDir;
    use walkdir::WalkDir;

    // Should get previously stored value.
    #[test]
    fn get_stored_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path())?;

        store.set("key1".to_owned(), "value1".to_owned())?;
        store.set("key2".to_owned(), "value2".to_owned())?;

        assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
        assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = KvStore::open(temp_dir.path())?;
        assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
        assert_eq!(store.get("key2".to_owned())?, Some("value2".to_owned()));

        Ok(())
    }

    // Should overwrite existent value.
    #[test]
    fn overwrite_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path())?;

        store.set("key1".to_owned(), "value1".to_owned())?;
        assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
        store.set("key1".to_owned(), "value2".to_owned())?;
        assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = KvStore::open(temp_dir.path())?;
        assert_eq!(store.get("key1".to_owned())?, Some("value2".to_owned()));
        store.set("key1".to_owned(), "value3".to_owned())?;
        assert_eq!(store.get("key1".to_owned())?, Some("value3".to_owned()));

        Ok(())
    }

    // Should get `None` when getting a non-existent key.
    #[test]
    fn get_non_existent_value() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path())?;

        store.set("key1".to_owned(), "value1".to_owned())?;
        assert_eq!(store.get("key2".to_owned())?, None);

        // Open from disk again and check persistent data.
        drop(store);
        let mut store = KvStore::open(temp_dir.path())?;
        assert_eq!(store.get("key2".to_owned())?, None);

        Ok(())
    }

    #[test]
    fn remove_non_existent_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path())?;
        assert!(store.remove("key1".to_owned()).is_err());
        Ok(())
    }

    #[test]
    fn remove_key() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path())?;
        store.set("key1".to_owned(), "value1".to_owned())?;
        assert!(store.remove("key1".to_owned()).is_ok());
        assert_eq!(store.get("key1".to_owned())?, None);
        Ok(())
    }

    // Insert data until total size of the directory decreases.
    // Test data correctness after compaction.
    #[test]
    fn compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path())?;

        let dir_size = || {
            let entries = WalkDir::new(temp_dir.path()).into_iter();
            let len: walkdir::Result<u64> = entries
                .map(|res| {
                    res.and_then(|entry| entry.metadata())
                        .map(|metadata| metadata.len())
                })
                .sum();
            len.expect("fail to get directory size")
        };

        let mut current_size = dir_size();
        for iter in 0..1000 {
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                let value = format!("{}", iter);
                store.set(key, value)?;
            }

            let new_size = dir_size();
            if new_size > current_size {
                current_size = new_size;
                continue;
            }
            // Compaction triggered.

            drop(store);
            // reopen and check content.
            let mut store = KvStore::open(temp_dir.path())?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(store.get(key)?, Some(format!("{}", iter)));
            }
            return Ok(());
        }

        panic!("No compaction detected");
    }
}
