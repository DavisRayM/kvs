#![deny(missing_docs)]
//! A networked key-value database library.
//!
//! The library provides building blocks that can be combined together
//! to build a fully-fledged networked key-value store able to store string
//! keys and values.
//!
//! The key-value database implementation utilizes a log-structured store.

/// Represents a key-value store.
pub struct KvStore {}

impl KvStore {
    /// Creates a new Key-Value store.
    pub fn new() -> Self {
        Self {}
    }

    /// Set value for a key string.
    pub fn set(&mut self, key: String, value: String) {
        unimplemented!()
    }

    /// Get the value of a `String` key.
    pub fn get(&self, key: String) -> Option<String> {
        unimplemented!()
    }

    /// Remove the value of a `String` key from the store. If it exists.
    pub fn remove(&mut self, key: String) {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Should get previously stored value
    #[test]
    fn get_stored_value() {
        let mut store = KvStore::new();

        store.set("key1".to_owned(), "value1".to_owned());
        store.set("key2".to_owned(), "value2".to_owned());

        assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
        assert_eq!(store.get("key2".to_owned()), Some("value2".to_owned()));
    }

    // Should overwrite existent value
    #[test]
    fn overwrite_value() {
        let mut store = KvStore::new();

        store.set("key1".to_owned(), "value1".to_owned());
        assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));

        store.set("key1".to_owned(), "value2".to_owned());
        assert_eq!(store.get("key1".to_owned()), Some("value2".to_owned()));
    }

    // Should get `None` when getting a non-existent key
    #[test]
    fn get_non_existent_value() {
        let mut store = KvStore::new();

        store.set("key1".to_owned(), "value1".to_owned());
        assert_eq!(store.get("key2".to_owned()), None);
    }

    #[test]
    fn remove_key() {
        let mut store = KvStore::new();

        store.set("key1".to_owned(), "value1".to_owned());
        store.remove("key1".to_owned());
        assert_eq!(store.get("key1".to_owned()), None);
    }
}
