use crate::errors::Error;
use async_trait::async_trait;

use libipld::{store::StoreParams, Block, Cid};
use std::error::Error as StdError;

#[async_trait]
pub trait BlockStore: Send + Sync + Sized + DBStore {
    type Params: StoreParams;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Box<dyn StdError + Send + Sync>> {
        let read_res = self.read(cid.to_bytes())?;
        match read_res {
            Some(bz) => Ok(Block::<Self::Params>::new(*cid, bz)?),
            None => Err(Box::new(Error::Other("Cid not in blockstore".to_string()))),
        }
    }
    fn insert(&self, block: &Block<Self::Params>) -> Result<(), Box<dyn StdError>> {
        let bytes = block.data();
        let cid = &block.cid().to_bytes();
        Ok(self.write(cid, bytes)?)
    }

    fn evict(&self, cid: &Cid) -> Result<(), Box<dyn StdError>> {
        Ok(self.delete(cid.to_bytes())?)
    }

    fn contains(&self, cid: &Cid) -> Result<bool, Box<dyn StdError>> {
        Ok(self.exists(cid.to_bytes())?)
    }
}

pub trait DBStore: Send + Sync {
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
