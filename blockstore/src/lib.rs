mod db;
mod errors;
mod types;

use libipld::DefaultParams;
// use async_trait::async_trait;

use libipld::{store::StoreParams, Block, Cid};
use std::error::Error as StdError;

/// Wrapper for database to handle inserting and retrieving ipld data with Cids
pub trait BlockStore: Send + Sync {
    type Params: StoreParams;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Box<dyn StdError>>;

    fn insert(&self, block: &Block<Self::Params>) -> Result<(), Box<dyn StdError>>;

    fn evict(&self, cid: &Cid) -> Result<(), Box<dyn StdError>>;

    fn contains(&self, cid: &Cid) -> Result<bool, Box<dyn StdError>>;
}

struct DbBlockStore<T: types::Store> {
    db: T,
}

impl<T: types::Store> BlockStore for DbBlockStore<T> {
    type Params = DefaultParams;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Box<dyn StdError>> {
        let read_res = self.db.read(cid.to_bytes())?;
        match read_res {
            Some(bz) => Ok(Block::<DefaultParams>::new(*cid, bz)?),
            None => Err(Box::new(errors::Error::Other(
                "Cid not in blockstore".to_string(),
            ))),
        }
    }
    fn insert(&self, block: &Block<Self::Params>) -> Result<(), Box<dyn StdError>> {
        let bytes = block.data();
        let cid = &block.cid().to_bytes();
        Ok(self.db.write(cid, bytes)?)
    }

    fn evict(&self, cid: &Cid) -> Result<(), Box<dyn StdError>> {
        Ok(self.db.delete(cid.to_bytes())?)
    }

    fn contains(&self, cid: &Cid) -> Result<bool, Box<dyn StdError>> {
        Ok(self.db.exists(cid.to_bytes())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::tests::DBPath;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;

    #[test]
    fn test_mem_recovers_block() {
        let mem_db = db::MemoryDB::default();
        let store = DbBlockStore { db: mem_db };

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
        let mem_db = db::MemoryDB::default();
        let store = DbBlockStore { db: mem_db };

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
    fn test_db_recovers_block() {
        let path = DBPath::new("get_test");
        let db = db::Db::open(path.as_ref()).unwrap();
        let store = DbBlockStore { db: db };

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
    fn test_db_delete_block() {
        let path = DBPath::new("evict_test");
        let db = db::Db::open(path.as_ref()).unwrap();
        let store = DbBlockStore { db: db };

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
