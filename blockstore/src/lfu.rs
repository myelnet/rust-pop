use crate::errors::Error;
use crate::types::*;
use libipld::DefaultParams;
use libipld::{store::StoreParams, Block, Cid};
use parking_lot::RwLock;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::error::Error as StdError;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::hint::unreachable_unchecked;
use std::num::NonZeroUsize;
use std::ptr::NonNull;

use std::sync::Arc;

use crate::lfu_freq_list::{remove_entry_pointer, FrequencyList, LfuEntry};

// // / A thread-safe `HashMap` that keeps track of LFU entries.
// #[derive(Debug, Default)]
// struct LookupMap {
//     map: RwLock<HashMap<u64, NonNull<LfuEntry>>>,
// }

// unsafe impl Send for LookupMap {}
// unsafe impl Sync for LookupMap {}
//
// impl LookupMap {
//     fn new() -> Self {
//         Self {
//             map: RwLock::new(HashMap::new()),
//         }
//     }
//
//     fn db_index<K>(key: K) -> u64
//     where
//         K: AsRef<[u8]>,
//     {
//         let mut hasher = DefaultHasher::new();
//         key.as_ref().hash::<DefaultHasher>(&mut hasher);
//         hasher.finish()
//     }
//
//     fn write<K>(&self, key: &K, value: NonNull<LfuEntry>) -> Result<(), Error>
//     where
//         K: AsRef<[u8]>,
//     {
//         self.map.write().insert(Self::db_index(key), value);
//         Ok(())
//     }
//
//     fn delete<K>(&self, key: &K) -> Result<Option<NonNull<LfuEntry>>, Error>
//     where
//         K: AsRef<[u8]>,
//     {
//         Ok(self.map.write().remove(&Self::db_index(key)))
//     }
//
//     fn read<K>(&self, key: &K) -> Result<Option<NonNull<LfuEntry>>, Error>
//     where
//         K: AsRef<[u8]>,
//     {
//         Ok(self.map.read().get(&Self::db_index(key)).cloned())
//     }
//
//     fn exists<K>(&self, key: &K) -> Result<bool, Error>
//     where
//         K: AsRef<[u8]>,
//     {
//         Ok(self.map.read().contains_key(&Self::db_index(key)))
//     }
//
//     // fn get_mut<K>(&mut self, key: &K) -> Result<Option<&mut NonNull<LfuEntry>>, Error>
//     // where
//     //     K: AsRef<[u8]>,
//     // {
//     //     Ok(self.map.read().get_mut(&Self::db_index(key)))
//     // }
// }

//  ---------------------------------------------------------------------------------------

struct LookupMap(HashMap<Arc<Vec<u8>>, NonNull<LfuEntry>>);

/// A blockstore that, if limited to a certain capacity, will evict based on the
/// least recently used value.
#[allow(clippy::module_name_repetitions)]
pub struct LfuBlockstore<B>
where
    B: DBStore,
{
    db: B,
    lookup: LookupMap,
    freq_list: FrequencyList,
    capacity: Option<NonZeroUsize>,
    len: usize,
}

unsafe impl<B: DBStore> Send for LfuBlockstore<B> {}
unsafe impl<B: DBStore> Sync for LfuBlockstore<B> {}

impl<B> LfuBlockstore<B>
where
    B: DBStore,
{
    /// Returns `Ok(true)` if key exists in store
    fn exists(&self, key: &Vec<u8>) -> Result<bool, Error> {
        self.db.exists(key)
    }

    fn total_size(&self) -> Result<usize, Error> {
        self.db.total_size()
    }
    /// Like [`Self::insert`], but with an shared key instead.
    fn write<V>(&mut self, key: Arc<Vec<u8>>, value: V) -> Result<(), Error>
    where
        V: AsRef<[u8]>,
    {
        self.delete(&key).unwrap();

        if let Some(capacity) = self.capacity {
            // After writing data - empty out old data !
            while capacity.get() <= self.total_size().unwrap() {
                self.pop_lfu();
            }
        }

        self.db.write(&*key, value).unwrap();

        // Since an entry has a reference to its key, we've created a situation
        // where we have self-referential data. We can't construct the entry
        // before inserting it into the lookup table because the key may be
        // moved when inserting it (so the memory address may become invalid)
        // but we can't insert the entry without constructing the value first.
        // let rc_key = Rc::new(*key);
        // let v = self.freq_list.insert(rc_key);
        // self.lookup.write(&key, v).unwrap();
        self.lookup.0.insert(Arc::clone(&key), NonNull::dangling());
        let v = self.lookup.0.get_mut(&key).unwrap();
        *v = self.freq_list.insert(key);

        // let v = self.lookup.get_mut(Rc::clone(&rc_key));
        // *v = self.freq_list.insert(rc_key);

        self.len += 1;

        Ok(())
    }

    /// Removes a value from the cache and blockstore by key, if it exists.
    #[inline]
    fn delete(&mut self, key: &Vec<u8>) -> Result<(), Error> {
        self.lookup.0.remove(key).map(|mut node| {
            // SAFETY: We have unique access to self. At this point, we've
            // removed the entry from the lookup map but haven't removed it from
            // the frequency data structure, so we need to clean it up there
            // // before returning the value.
            remove_entry_pointer(
                *unsafe { Box::from_raw(node.as_mut()) },
                &mut self.freq_list,
                &mut self.len,
            )
        });
        self.db.delete(key).unwrap();

        Ok(())
    }

    /// Gets a value and incrementing the internal frequency counter of that
    /// value, if it exists.
    #[inline]
    fn read(&mut self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        let entry = self.lookup.0.get(key).unwrap();
        self.freq_list.update(*entry);
        // SAFETY: This is fine because self is uniquely borrowed
        self.db.read(key)
    }
}

impl<B> LfuBlockstore<B>
where
    B: DBStore,
{
    #[inline]
    #[must_use]
    pub fn new(capacity: usize, bs: B) -> Self {
        Self {
            db: bs,
            lookup: LookupMap(HashMap::new()),
            freq_list: FrequencyList::new(),
            capacity: NonZeroUsize::new(capacity),
            len: 0,
        }
    }

    pub fn increment_len(&mut self) -> () {
        self.len += 1;
    }

    /// Evicts the least frequently used value and returns it. If the cache is
    /// empty, then this returns None. If there are multiple items that have an
    /// equal access count, then the most recently added value is evicted.
    #[inline]
    pub fn pop_lfu(&mut self) -> () {
        self.pop_lfu_key_value_frequency().unwrap()
    }

    /// Evicts the least frequently used value and returns it, the key it was
    /// inserted under, and the frequency it had. If the cache is empty, then
    /// this returns None. If there are multiple items that have an equal access
    /// count, then the most recently added key-value pair is evicted.
    #[inline]
    pub fn pop_lfu_key_value_frequency(&mut self) -> Option<()> {
        self.freq_list.pop_lfu().map(|mut entry_ptr| {
            // SAFETY: This is fine since self is uniquely borrowed.
            let key = unsafe { entry_ptr.as_ref().key.as_ref() };
            self.db.delete(key).unwrap();

            // SAFETY: entry_ptr is guaranteed to be a live reference and is
            // is separated from the data structure as a guarantee of pop_lfu.
            // As a result, at this point, we're guaranteed that we have the
            // only reference of entry_ptr.
            let entry = unsafe { Box::from_raw(entry_ptr.as_mut()) };
            let _key = match Arc::try_unwrap(entry.key) {
                Ok(k) => k,
                Err(_) => unsafe { unreachable_unchecked() },
            };
            ()
        })
    }
}

impl<B: DBStore> BlockStore for LfuBlockstore<B> {
    type Params = DefaultParams;

    fn get(&mut self, cid: &Cid) -> Result<Block<Self::Params>, Box<dyn StdError + Send + Sync>> {
        let read_res = self.read(&cid.to_bytes())?;
        match read_res {
            Some(bz) => Ok(Block::<Self::Params>::new(*cid, bz)?),
            None => Err(Box::new(Error::Other("Cid not in blockstore".to_string()))),
        }
    }
    fn insert(&mut self, block: &Block<Self::Params>) -> Result<(), Box<dyn StdError>> {
        let bytes = block.data();
        let cid = block.cid().to_bytes();
        Ok(self.write(Arc::new(cid), bytes)?)
    }

    fn evict(&mut self, cid: &Cid) -> Result<(), Box<dyn StdError>> {
        Ok(self.delete(&cid.to_bytes())?)
    }

    fn contains(&self, cid: &Cid) -> Result<bool, Box<dyn StdError>> {
        Ok(self.exists(&cid.to_bytes())?)
    }
}

#[cfg(test)]
mod blockstore_tests {
    use super::LfuBlockstore;
    use crate::memory::MemoryDB;
    use crate::types::{BlockStore, DBStore};
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;
    use libipld::Block;
    use std::sync::Arc;

    pub fn test_write<B: DBStore>(db: &mut LfuBlockstore<B>) {
        let key = Arc::new([0x41u8, 0x41u8, 0x42u8].to_vec());
        let value = [1];
        db.write(key, value).unwrap();
    }

    pub fn test_read<B: DBStore>(db: &mut LfuBlockstore<B>) {
        let key = Arc::new([0x41u8, 0x41u8, 0x42u8].to_vec());
        let value = [1];
        db.write(key.clone(), value).unwrap();
        let res = db.read(&key).unwrap().unwrap();
        assert_eq!(value.as_ref(), res.as_slice());
    }

    pub fn test_exists<B: DBStore>(db: &mut LfuBlockstore<B>) {
        let key = Arc::new([0x41u8, 0x41u8, 0x42u8].to_vec());
        let value = [1];
        db.write(key.clone(), value).unwrap();
        let res = db.exists(&key).unwrap();
        assert_eq!(res, true);
    }

    pub fn test_does_not_exist<B: DBStore>(db: &LfuBlockstore<B>) {
        let key = Arc::new([0x41u8, 0x41u8, 0x42u8].to_vec());
        let res = db.exists(&key).unwrap();
        assert_eq!(res, false);
    }

    pub fn test_delete<B: DBStore>(db: &mut LfuBlockstore<B>) {
        let key = Arc::new([0x41u8, 0x41u8, 0x42u8].to_vec());
        let value = [1];
        db.write(key.clone(), value).unwrap();
        let res = db.exists(&key).unwrap();
        assert_eq!(res, true);
        db.delete(&key).unwrap();
        let res = db.exists(&key).unwrap();
        assert_eq!(res, false);
    }

    #[test]
    fn mem_db_write() {
        let db = MemoryDB::default();
        let mut lfu = LfuBlockstore::new(0, db);
        test_write(&mut lfu);
    }

    #[test]
    fn mem_db_read() {
        let db = MemoryDB::default();
        let mut lfu = LfuBlockstore::new(0, db);
        test_read(&mut lfu);
    }

    #[test]
    fn mem_db_exists() {
        let db = MemoryDB::default();
        let mut lfu = LfuBlockstore::new(0, db);
        test_exists(&mut lfu);
    }

    #[test]
    fn mem_db_does_not_exist() {
        let db = MemoryDB::default();
        let mut lfu = LfuBlockstore::new(0, db);
        test_does_not_exist(&mut lfu);
    }

    #[test]
    fn mem_db_delete() {
        let db = MemoryDB::default();
        let mut lfu = LfuBlockstore::new(0, db);
        test_delete(&mut lfu);
    }

    #[test]
    fn test_mem_recovers_block() {
        let mut store = LfuBlockstore::new(0, MemoryDB::default());

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
        let mut store = LfuBlockstore::new(0, MemoryDB::default());

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
}
