use crate::errors::Error;
use crate::types::BlockStore;
use libipld::{Block, Cid};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// A memory blockstore backed by a persistent shared blockstore.
/// blocks inserted are stored in memory until flushed to the inner blockstore or evicted.
/// the cache size is capped at a given size so any additional element will cause oldest
/// blocks to be evicted. Default watermark is 16MiB.
pub struct TempBlockStore<S> {
    data: RwLock<HashMap<Cid, Vec<u8>>>,
    queue: RwLock<VecDeque<Cid>>,
    capacity: Arc<AtomicUsize>,
    max_cap: usize,
    inner: Arc<S>,
}

impl<S> TempBlockStore<S>
where
    S: BlockStore,
{
    pub fn new(store: Arc<S>) -> Self {
        Self {
            data: RwLock::new(HashMap::default()),
            capacity: Arc::new(AtomicUsize::new(0)),
            queue: RwLock::new(VecDeque::new()),
            max_cap: 16 * 1024 * 1024,
            inner: store,
        }
    }

    /// sets the maximum cache size.
    pub fn max_capacity(mut self, cap: usize) -> Self {
        self.max_cap = cap;
        self
    }

    /// flush cached blocks to the inner blockstore.
    pub fn flush(&self, cid: &Cid) -> Result<(), Error> {
        if let Some(data) = self.data.write().remove(cid) {
            self.capacity.fetch_sub(data.len(), Ordering::Relaxed);
            self.remove_from_queue(cid);
            self.inner
                .insert(&Block::<S::Params>::new_unchecked(*cid, data))
        } else {
            Err(Error::BlockNotFound)
        }
    }

    /// returns a block if it is present in the cache.
    pub fn get_from_cache(&self, cid: &Cid) -> Result<Block<S::Params>, Error> {
        if let Some(data) = self.data.read().get(cid) {
            return Ok(Block::<S::Params>::new_unchecked(*cid, data.clone()));
        }
        Err(Error::BlockNotFound)
    }

    /// returns the current cache size.
    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    fn remove_from_queue(&self, cid: &Cid) {
        let mut q = self.queue.write();
        // binary search only works if the queue is sorted
        q.make_contiguous().sort();
        if let Ok(idx) = q.binary_search(cid) {
            q.remove(idx).expect("item should be in the queue");
        }
    }
}

impl<S> BlockStore for TempBlockStore<S>
where
    S: BlockStore,
{
    type Params = S::Params;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Error> {
        // try the cache first and fallback to blockstore
        match self.get_from_cache(cid) {
            Ok(block) => Ok(block),
            Err(_) => self.inner.get(cid),
        }
    }
    fn insert(&self, block: &Block<Self::Params>) -> Result<(), Error> {
        let (cid, data) = block.clone().into_inner();
        self.data.write().insert(cid, data);
        self.queue.write().push_back(cid);
        // fetch_add returns the previous value so we still add the operation to the previous value
        let mut capacity = self
            .capacity
            .fetch_add(block.data().len(), Ordering::Relaxed)
            + block.data().len();
        // evict any content if we reached the watermark
        while capacity > self.max_cap {
            match self.queue.write().pop_front() {
                Some(cid) => {
                    let size = self
                        .data
                        .write()
                        .remove(&cid)
                        .expect("should have data for queued cid")
                        .len();
                    capacity = self.capacity.fetch_sub(size, Ordering::Relaxed) - size;
                }
                None => break,
            }
        }
        Ok(())
    }
    fn evict(&self, cid: &Cid) -> Result<(), Error> {
        match self.data.write().remove(cid) {
            Some(data) => {
                self.capacity.fetch_sub(data.len(), Ordering::Relaxed);
                self.remove_from_queue(cid);
                Ok(())
            }
            None => Err(Error::BlockNotFound),
        }
    }
    fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        Ok(self.data.read().contains_key(cid))
    }
}

#[cfg(test)]
mod tests {
    use super::super::memory::MemoryDB;
    use super::*;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::Code;
    use libipld::{ipld, Block, DefaultParams};

    #[test]
    fn tempstore_capacity() {
        let store = Arc::new(MemoryDB::default());

        let node1 = ipld!({ "some": "content that should be about 55 bytes blablabla" });
        let block1 = Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &node1).unwrap();

        let node2 = ipld!({ "more": "content that should be about 45 bytes"});
        let block2 = Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &node2).unwrap();

        let node3 =
            ipld!({ "this": "block is even larger with over 70 bytes and counting 123456789" });
        let block3 = Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &node3).unwrap();

        let tstore = TempBlockStore::new(store).max_capacity(120);

        tstore.insert(&block1).unwrap();

        assert_eq!(tstore.capacity(), 55);

        tstore.insert(&block2).unwrap();

        assert_eq!(tstore.capacity(), 100);

        tstore.insert(&block3).unwrap();

        assert_eq!(tstore.capacity(), 115);

        assert_eq!(tstore.queue.read().len(), 2);

        // block1 should have been evicted from the cache
        let result = tstore.get(block1.cid());
        assert_eq!(result, Err(Error::BlockNotFound));

        tstore.flush(block2.cid()).unwrap();

        // block2 should have been evicted from the cache
        let result = tstore.get_from_cache(block2.cid());
        assert_eq!(result, Err(Error::BlockNotFound));

        // block2 should still be available in the inner store
        let _ = tstore.get(block2.cid()).unwrap();

        tstore.evict(block3.cid()).unwrap();

        let result = tstore.get(block3.cid());
        assert_eq!(result, Err(Error::BlockNotFound));

        assert_eq!(tstore.capacity(), 0);

        assert_eq!(tstore.queue.read().len(), 0);
    }
}
