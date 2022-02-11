use crate::errors::Error;
use async_trait::async_trait;

use libipld::{store::StoreParams, Block, Cid};
use std::error::Error as StdError;
use std::iter::FromIterator;

#[async_trait]
pub trait BlockStore: Send + Sync + Sized {
    type Params: StoreParams;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Box<dyn StdError + Send + Sync>>;
    fn insert(&self, block: &Block<Self::Params>) -> Result<(), Box<dyn StdError>>;

    fn evict(&self, cid: &Cid) -> Result<(), Box<dyn StdError>>;

    fn contains(&self, cid: &Cid) -> Result<bool, Box<dyn StdError>>;
}

pub trait DBStore: Send + Sync {
    fn key_iterator<I: FromIterator<Vec<u8>>>(&self) -> Result<I, Error>;

    fn total_size(&self) -> Result<usize, Error>;

    fn read<K>(&self, key: K) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]>;

    /// Write a single value to the data store.
    fn write<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    /// Delete value at key.
    fn delete<K>(&self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>;

    /// Returns `Ok(true)` if key exists in store
    fn exists<K>(&self, key: K) -> Result<bool, Error>
    where
        K: AsRef<[u8]>;

    /// Read slice of keys and return a vector of optional values.
    fn bulk_read<K>(&self, keys: &[K]) -> Result<Vec<Option<Vec<u8>>>, Error>
    where
        K: AsRef<[u8]>,
    {
        keys.iter().map(|key| self.read(key)).collect()
    }

    /// Write slice of KV pairs.
    fn bulk_write<K, V>(&self, values: &[(K, V)]) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        values
            .iter()
            .try_for_each(|(key, value)| self.write(key, value))
    }

    /// Bulk delete keys from the data store.
    fn bulk_delete<K>(&self, keys: &[K]) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        keys.iter().try_for_each(|key| self.delete(key))
    }
}
