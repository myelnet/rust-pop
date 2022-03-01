use crate::errors::Error;
use crate::types::{BlockStore, DBStore};
use libipld::DefaultParams;
use libipld::{Block, Cid};
use parking_lot::RwLock;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::iter;
use std::mem;
/// A thread-safe `HashMap` wrapper.
#[derive(Debug, Default)]
pub struct MemoryDB {
    pub db: RwLock<HashMap<u64, Vec<u8>>>,
    /// only set for testing purposes
    pub keys: Option<RwLock<HashMap<Vec<u8>, u64>>>,
}

impl MemoryDB {
    fn db_index<K>(key: K) -> u64
    where
        K: AsRef<[u8]>,
    {
        let mut hasher = DefaultHasher::new();
        key.as_ref().hash::<DefaultHasher>(&mut hasher);
        hasher.finish()
    }
}

impl Clone for MemoryDB {
    fn clone(&self) -> Self {
        Self {
            db: RwLock::new(self.db.read().clone()),
            keys: None,
        }
    }
}

impl DBStore for MemoryDB {
    fn key_iterator<I: FromIterator<Vec<u8>>>(&self) -> Result<I, Error> {
        match &self.keys {
            Some(keys) => Ok(keys.read().iter().map(|(k, _)| k.clone()).collect()),
            None => Ok(iter::empty::<Vec<u8>>().collect()),
        }
    }

    fn total_size(&self) -> Result<usize, Error> {
        Ok(mem::size_of_val(&self.db.read()))
    }

    fn write<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        match &self.keys {
            Some(keys) => {
                keys.write()
                    .insert(key.as_ref().to_vec(), Self::db_index(&key));
            }
            None => {}
        }

        self.db
            .write()
            .insert(Self::db_index(key), value.as_ref().to_vec());

        Ok(())
    }

    fn delete<K>(&self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        match &self.keys {
            Some(keys) => {
                keys.write().remove(&key.as_ref().to_vec());
            }
            None => {}
        }

        self.db.write().remove(&Self::db_index(key));
        Ok(())
    }

    fn read<K>(&self, key: K) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]>,
    {
        Ok(self.db.read().get(&Self::db_index(key)).cloned())
    }

    fn exists<K>(&self, key: K) -> Result<bool, Error>
    where
        K: AsRef<[u8]>,
    {
        Ok(self.db.read().contains_key(&Self::db_index(key)))
    }
}

impl BlockStore for MemoryDB {
    type Params = DefaultParams;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Error> {
        let read_res = self.read(cid.to_bytes())?;
        match read_res {
            Some(bz) => Ok(Block::<Self::Params>::new_unchecked(*cid, bz)),
            None => Err(Error::BlockNotFound(*cid)),
        }
    }
    fn insert(&self, block: &Block<Self::Params>) -> Result<(), Error> {
        let bytes = block.data();
        let cid = &block.cid().to_bytes();
        Ok(self.write(cid, bytes)?)
    }

    fn evict(&self, cid: &Cid) -> Result<(), Error> {
        Ok(self.delete(cid.to_bytes())?)
    }

    fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        Ok(self.exists(cid.to_bytes())?)
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;
    use libipld::Block;

    use crate::test_helpers::*;

    #[test]
    fn mem_db_write() {
        let mut db = MemoryDB::default();
        test_write(&mut db);
    }

    #[test]
    fn mem_db_read() {
        let mut db = MemoryDB::default();
        test_read(&mut db);
    }

    #[test]
    fn mem_db_exists() {
        let mut db = MemoryDB::default();
        test_exists(&mut db);
    }

    #[test]
    fn mem_db_does_not_exist() {
        let mut db = MemoryDB::default();
        test_does_not_exist(&mut db);
    }

    #[test]
    fn mem_db_delete() {
        let mut db = MemoryDB::default();
        test_delete(&mut db);
    }

    #[test]
    fn mem_db_bulk_write() {
        let mut db = MemoryDB::default();
        test_bulk_write(&mut db);
    }

    #[test]
    fn mem_db_bulk_read() {
        let mut db = MemoryDB::default();
        test_bulk_read(&mut db);
    }

    #[test]
    fn mem_db_bulk_delete() {
        let mut db = MemoryDB::default();
        test_bulk_delete(&mut db);
    }

    #[test]
    fn test_mem_recovers_block() {
        let store = MemoryDB::default();

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();
        store.insert(&leaf1_block).unwrap();

        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();
        store.insert(&leaf2_block).unwrap();

        let leaf1_recovered_block = store.get(leaf1_block.cid()).unwrap();
        let leaf2_recovered_block = store.get(leaf2_block.cid()).unwrap();

        assert_eq!(leaf1_block, leaf1_recovered_block);
        assert_eq!(leaf2_block, leaf2_recovered_block);
    }

    #[test]
    fn test_mem_delete_block() {
        let store = MemoryDB::default();

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();
        store.insert(&leaf1_block.clone()).unwrap();

        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();
        store.insert(&leaf2_block.clone()).unwrap();

        store.evict(leaf1_block.cid()).unwrap();
        store.evict(leaf2_block.cid()).unwrap();

        let exists_leaf1 = store.contains(leaf1_block.cid()).unwrap();
        let exists_leaf2 = store.contains(leaf2_block.cid()).unwrap();

        assert_eq!(false, exists_leaf1);
        assert_eq!(false, exists_leaf2);
    }

    #[test]
    fn test_mem_keys() {
        let db = MemoryDB {
            db: RwLock::new(HashMap::new()),
            keys: Some(RwLock::new(HashMap::new())),
        };
        db.write(Vec::from([1u8]), Vec::from([2u8])).unwrap();
        db.write(Vec::from([2u8]), Vec::from([3u8])).unwrap();
        db.write(Vec::from([3u8]), Vec::from([4u8])).unwrap();

        let keys = db.key_iterator::<Vec<Vec<u8>>>().unwrap();
        assert!(keys.contains(&Vec::from([1u8])));
        assert!(keys.contains(&Vec::from([2u8])));
        assert!(keys.contains(&Vec::from([3u8])));
    }
}
