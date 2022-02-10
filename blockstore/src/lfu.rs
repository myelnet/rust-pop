use crate::errors::Error;
use crate::types::*;
use libipld::DefaultParams;
use libipld::{Block, Cid};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::num::NonZeroUsize;
use std::ptr::NonNull;

use std::sync::Arc;

use crate::lfu_freq_list::{remove_entry_pointer, FrequencyList, LfuEntry};
use std::mem;

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
    
    fn exists(&self, key: &Vec<u8>) -> Result<bool, Error> {
        self.db.exists(key)
    }

    fn total_size(&self) -> Result<usize, Error> {
        self.db.total_size()
    }
    fn write<V>(&mut self, key: Arc<Vec<u8>>, value: V) -> Result<(), Error>
    where
        V: AsRef<[u8]>,
    {
        // Protects against overly large files emptying out the store entirely
        if let Some(capacity) = self.capacity {
            if capacity.get() <= mem::size_of_val(&value) {
                return Ok(());
            }
        }

        self.delete(&key).unwrap();

        self.db.write(&*key, value).unwrap();

        if let Some(capacity) = self.capacity {
            // After writing data - empty out old data !
            while capacity.get() <= self.total_size().unwrap() {
                println!("{:?}", self.total_size().unwrap());
                self.pop_lfu();
            }
        }

        // Since an entry has a reference to its key, we've created a situation
        // where we have self-referential data. We can't construct the entry
        // before inserting it into the lookup table because the key may be
        // moved when inserting it (so the memory address may become invalid)
        // but we can't insert the entry without constructing the value first.
        self.lookup.0.insert(Arc::clone(&key), NonNull::dangling());
        let v = self.lookup.0.get_mut(&key).unwrap();
        *v = self.freq_list.insert(key);

        self.len += 1;

        Ok(())
    }

    /// Removes a value from the lfu and blockstore by key, if it exists.

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

    fn read(&mut self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        match self.lookup.0.get(key) {
            Some(entry) => {
                self.freq_list.update(*entry);
                return self.db.read(key);
            }
            None => Ok(None),
        }
    }



    /// Evicts the least frequently used key and returns it. If the lfu is
    /// empty, then this returns None. If there are multiple items that have an
    /// equal access count, then the most recently added value is evicted.

    pub fn pop_lfu(&mut self) -> Option<&Vec<u8>> {
        self.pop_lfu_key_value()
    }

    /// Evicts the least frequently used key and returns it. If the lfu is empty, then
    /// this returns None. If there are multiple items that have an equal access
    /// count, then the most recently added key-value pair is evicted.

    pub fn pop_lfu_key_value(&mut self) -> Option<&Vec<u8>> {
        self.freq_list.pop_lfu().map(|entry_ptr| {
            // SAFETY: This is fine since self is uniquely borrowed.
            let key = unsafe { entry_ptr.as_ref().key.as_ref() };
            self.db.delete(key).unwrap();
            key
        })
    }

    /// Returns the frequencies that this lfu has. This is a linear time
    /// operation.

    #[must_use]
    pub fn frequencies(&self) -> Vec<usize> {
        self.freq_list.frequencies()
    }

    /// Returns the current number of items in the lfu. This is a constant
    /// time operation.

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
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
mod blockstore {
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

#[cfg(test)]
mod read {
    use super::LfuBlockstore;
    use crate::memory::MemoryDB;
    use std::sync::Arc;

    #[test]
    fn empty() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        for i in 0..100 {
            match lfu.read(&Vec::from([i as u8])).unwrap() {
                Some(_value) => {
                    assert!(false)
                }
                None => {}
            }
        }
    }

    #[test]
    fn getting_is_ok_after_adding_other_value() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        let key1 = Arc::new(Vec::from([1u8]));
        let v1 = Vec::from([2]);
        lfu.write(key1.clone(), &v1).unwrap();
        assert_eq!(lfu.read(&key1).unwrap(), Some(v1));
        let key2 = Arc::new(Vec::from([3u8]));
        let v2 = Vec::from([4]);
        lfu.write(key2.clone(), &v2).unwrap();
        assert_eq!(lfu.read(&key2).unwrap(), Some(v2));
    }

    #[test]
    fn bounded_alternating_values() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        let key1 = Arc::new(Vec::from([1u8]));
        let v1 = Vec::from([1]);
        let key2 = Arc::new(Vec::from([2u8]));
        let v2 = Vec::from([2]);
        lfu.write(key1, &v1).unwrap();
        lfu.write(key2, &v2).unwrap();
        for _ in 0..100 {
            lfu.read(&v1).unwrap();
            lfu.read(&v2).unwrap();
        }

        assert_eq!(lfu.len(), 2);
        assert_eq!(lfu.frequencies(), vec![100]);
    }
}

#[cfg(test)]
mod write {
    use super::LfuBlockstore;
    use crate::memory::MemoryDB;
    use std::sync::Arc;

    #[test]
    fn insert_unbounded() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());

        for i in 0..100 {
            let k = Arc::new(Vec::from([i as u8]));
            let v = Vec::from([i + 100 as u8]);
            lfu.write(k, v).unwrap();
        }

        for i in 0..100 {
            let k = Arc::new(Vec::from([i as u8]));
            let v = Vec::from([i + 100 as u8]);
            assert_eq!(lfu.read(&k).unwrap(), Some(v.clone()));
            assert!(lfu.read(&Arc::new(v)).unwrap().is_none());
        }
    }

    #[test]
    fn reinsertion_of_same_key_resets_freq() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        let k = Arc::new(Vec::from([1u8]));
        let v = Vec::from([1]);
        lfu.write(k.clone(), &v).unwrap();
        lfu.read(&k).unwrap();
        lfu.write(k, v).unwrap();
        assert_eq!(lfu.frequencies(), vec![0]);
    }

    #[test]
    fn insert_bounded() {
        let mut lfu = LfuBlockstore::new(10, MemoryDB::default());

        for i in 0..100 {
            let k = Arc::new(Vec::from([i as u8]));
            let v = Vec::from([i + 100 as u8]);
            lfu.write(k, v).unwrap();
        }
    }
}

#[cfg(test)]
mod pop {
    use super::LfuBlockstore;
    use crate::memory::MemoryDB;
    use std::sync::Arc;

    #[test]
    fn pop() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        for i in 0..100 {
            let k = Arc::new(Vec::from([i as u8]));
            let v = Vec::from([i + 100 as u8]);
            lfu.write(k, &v).unwrap();
        }

        for i in 0..100 {
            assert_eq!(lfu.lookup.0.len(), 100);
            let evicted_index = 100 - i - 1;
            let k = Vec::from([evicted_index as u8]);
            assert_eq!(lfu.pop_lfu(), Some(&k));
        }
    }

    #[test]
    fn pop_empty() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        assert_eq!(None, lfu.pop_lfu());
    }
}

#[cfg(test)]
mod delete {
    use super::LfuBlockstore;
    use crate::memory::MemoryDB;
    use std::sync::Arc;

    #[test]
    fn remove_to_empty() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([2u8]))
            .unwrap();
        lfu.delete(&Vec::from([1u8])).unwrap();
        assert!(lfu.len() == 0);
        assert_eq!(lfu.freq_list.len, 0);
    }

    #[test]
    fn remove_empty() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.delete(&Vec::from([1u8])).unwrap();
    }

    #[test]
    fn remove_to_nonempty() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([2u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([3u8])), Vec::from([4u8]))
            .unwrap();

        lfu.delete(&Vec::from([1u8])).unwrap();

        assert!(!(lfu.len() == 0));

        lfu.delete(&Vec::from([3u8])).unwrap();

        assert!(lfu.len() == 0);

        assert_eq!(lfu.freq_list.len, 0);
    }

    #[test]
    fn remove_middle() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([2u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([3u8])), Vec::from([4u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([5u8])), Vec::from([6u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([7u8])), Vec::from([8u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([9u8])), Vec::from([10u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([11u8])), Vec::from([12u8]))
            .unwrap();

        lfu.read(&Vec::from([7u8])).unwrap();
        lfu.read(&Vec::from([9u8])).unwrap();
        lfu.read(&Vec::from([11u8])).unwrap();

        assert_eq!(lfu.frequencies(), vec![0, 1]);
        assert_eq!(lfu.len(), 6);

        lfu.delete(&Vec::from([9u8])).unwrap();
        assert!(lfu.read(&Vec::from([7u8])).unwrap().is_some());
        assert!(lfu.read(&Vec::from([11u8])).unwrap().is_some());

        lfu.delete(&Vec::from([3u8])).unwrap();
        assert!(lfu.read(&Vec::from([1u8])).unwrap().is_some());
        assert!(lfu.read(&Vec::from([5u8])).unwrap().is_some());
    }

    #[test]
    fn remove_end() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([2u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([3u8])), Vec::from([4u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([5u8])), Vec::from([6u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([7u8])), Vec::from([8u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([9u8])), Vec::from([10u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([11u8])), Vec::from([12u8]))
            .unwrap();

        lfu.read(&Vec::from([7u8])).unwrap();
        lfu.read(&Vec::from([9u8])).unwrap();
        lfu.read(&Vec::from([11u8])).unwrap();

        assert_eq!(lfu.frequencies(), vec![0, 1]);
        assert_eq!(lfu.len(), 6);

        lfu.delete(&Vec::from([7u8])).unwrap();
        assert!(lfu.read(&Vec::from([9u8])).unwrap().is_some());
        assert!(lfu.read(&Vec::from([11u8])).unwrap().is_some());

        lfu.delete(&Vec::from([1u8])).unwrap();
        assert!(lfu.read(&Vec::from([3u8])).unwrap().is_some());
        assert!(lfu.read(&Vec::from([5u8])).unwrap().is_some());
    }

    #[test]
    fn remove_start() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([2u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([3u8])), Vec::from([4u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([5u8])), Vec::from([6u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([7u8])), Vec::from([8u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([9u8])), Vec::from([10u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([11u8])), Vec::from([12u8]))
            .unwrap();

        lfu.read(&Vec::from([7u8])).unwrap();
        lfu.read(&Vec::from([9u8])).unwrap();
        lfu.read(&Vec::from([11u8])).unwrap();

        assert_eq!(lfu.frequencies(), vec![0, 1]);
        assert_eq!(lfu.len(), 6);

        lfu.delete(&Vec::from([11u8])).unwrap();
        assert!(lfu.read(&Vec::from([9u8])).unwrap().is_some());
        assert!(lfu.read(&Vec::from([7u8])).unwrap().is_some());

        lfu.delete(&Vec::from([5u8])).unwrap();
        assert!(lfu.read(&Vec::from([3u8])).unwrap().is_some());
        assert!(lfu.read(&Vec::from([1u8])).unwrap().is_some());
    }

    #[test]
    fn remove_connects_next_owner() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([1u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([2u8])), Vec::from([2u8]))
            .unwrap();
        assert_eq!(lfu.read(&Vec::from([1u8])).unwrap(), Some(Vec::from([1u8])));
        lfu.delete(&Vec::from([2u8])).unwrap();
        assert_eq!(lfu.read(&Vec::from([1u8])).unwrap(), Some(Vec::from([1u8])));
    }
}

#[cfg(test)]
mod bookkeeping {
    use super::LfuBlockstore;
    use crate::memory::MemoryDB;
    use std::sync::Arc;

    #[test]
    fn getting_one_element_has_constant_freq_list_size() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([2u8]))
            .unwrap();
        assert_eq!(lfu.freq_list.len, 1);

        for _ in 0..100 {
            lfu.read(&Vec::from([1u8])).unwrap();
            assert_eq!(lfu.freq_list.len, 1);
        }
    }

    #[test]
    fn freq_list_node_merges() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([2u8]))
            .unwrap();
        lfu.write(Arc::new(Vec::from([3u8])), Vec::from([4u8]))
            .unwrap();
        assert_eq!(lfu.freq_list.len, 1);
        assert!(lfu.read(&Vec::from([1u8])).unwrap().is_some());
        assert_eq!(lfu.freq_list.len, 2);
        assert!(lfu.read(&Vec::from([3u8])).unwrap().is_some());
        assert_eq!(lfu.freq_list.len, 1);
    }

    #[test]
    fn freq_list_multi_items() {
        let mut lfu = LfuBlockstore::new(0, MemoryDB::default());
        lfu.write(Arc::new(Vec::from([1u8])), Vec::from([2u8]))
            .unwrap();
        lfu.read(&Vec::from([1u8])).unwrap();
        lfu.read(&Vec::from([1u8])).unwrap();
        lfu.write(Arc::new(Vec::from([3u8])), Vec::from([4u8]))
            .unwrap();
        assert_eq!(lfu.freq_list.len, 2);
        lfu.read(&Vec::from([3u8])).unwrap();
        assert_eq!(lfu.freq_list.len, 2);
        lfu.read(&Vec::from([3u8])).unwrap();
        assert_eq!(lfu.freq_list.len, 1);
    }
}
