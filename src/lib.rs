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
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::PathBuf,
};

/// File extension for logs
pub const LOG_EXTENSION: &str = ".kv";
const COMPACTION_THRESHOLD: usize = 10;

/// Custom `Result` type that represents a success or error of KvStore
/// functionality
pub type Result<T> = std::result::Result<T, StoreError>;

/// The error type for KvStore operations.
#[derive(Debug)]
pub enum StoreError {
    /// An IO Error occured while accessing the underlying file.
    Io(std::io::Error),
    /// A serde error occured while serializing or deserializing a log entry.
    Serde(serde_json::error::Error),
    /// An operation failed due to a missing key. Often occurs when
    /// trying to remove a key that does not exist
    NotFound,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Io(err) => write!(f, "IO Error: {}", err),
            StoreError::NotFound => write!(f, "Key not found"),
            StoreError::Serde(err) => write!(f, "Serde Error: {}", err),
        }
    }
}

impl std::error::Error for StoreError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        match self {
            StoreError::Io(err) => Some(err),
            StoreError::NotFound => None,
            StoreError::Serde(err) => Some(err),
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

/// A list specifying supported Write-Ahead Log(WAL) entries.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum LogEntry {
    Set { key: String, value: String },
    Rm { key: String },
}

/// Represents the location of an entry in the log fragments.
#[derive(Debug, Clone)]
pub struct EntryPosition {
    /// Fragment the entry is currently located in.
    pub fragment: u64,
    /// Position of the entry in the fragment
    pub pos: u64,
    /// Size of the entry
    pub size: usize,
}

impl From<(u64, Range<u64>)> for EntryPosition {
    fn from(value: (u64, Range<u64>)) -> Self {
        Self {
            fragment: value.0,
            pos: value.1.start,
            size: (value.1.end - value.1.start) as usize,
        }
    }
}

/// Represents a key-value store.
pub struct KvStore {
    fragment: u64,
    index: HashMap<String, EntryPosition>,
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
        let fragment = 0;
        let path = dir_path.join(format!("{}{}", fragment, LOG_EXTENSION));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let mut reader = BufReader::new(file.try_clone()?);
        let writer = BufWriter::new(file);
        let mut index = HashMap::new();

        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut de = serde_json::Deserializer::from_reader(&mut reader).into_iter();
        while let Some(res) = de.next() {
            let entry: LogEntry = res?;
            let new_pos = de.byte_offset() as u64;
            match entry {
                LogEntry::Set { key, .. } => {
                    index.insert(key.to_owned(), (fragment, pos..new_pos).into())
                }
                LogEntry::Rm { ref key } => index.remove(key),
            };
            pos = new_pos;
        }

        Ok(Self {
            fragment,
            index,
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
        let buf = serde_json::to_vec(&entry)?;
        let size = buf.len() as u64;

        let pos = self.writer.seek(SeekFrom::End(0))?;
        let new_pos = size + pos;
        self.writer.write_all(&buf)?;
        self.writer.flush()?;

        self.index.insert(key, (self.fragment, pos..new_pos).into());
        Ok(())
    }

    /// Get the value of a key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(ep) => {
                self.reader.seek(SeekFrom::Start(ep.pos))?;

                let mut buf = Vec::new();
                buf.resize(ep.size, 0);
                self.reader.read(&mut buf[..])?;

                match serde_json::from_slice(&buf[..]) {
                    Ok(LogEntry::Set { value, .. }) => Ok(Some(value)),
                    // NOTE: This isn't expected; if this occurs there is something
                    //       horribly wrong with the position or in-memory index.
                    e => panic!("unexpected log entry at byte offset {}; {:?}", ep.pos, e),
                }
            }
            None => Ok(None),
        }
    }

    /// Remove the value of a key from the store, If it exists.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.get(key.clone())?
            .ok_or(StoreError::NotFound)
            .and_then(|_| {
                let entry = LogEntry::Rm { key: key.clone() };
                self.writer.seek(SeekFrom::End(0))?;
                serde_json::to_writer(&mut self.writer, &entry)?;
                self.writer.flush()?;
                self.index.remove(&key);
                Ok(())
            })
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
