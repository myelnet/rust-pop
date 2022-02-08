mod db;
mod errors;
mod types;

use libipld::DefaultParams;
// use async_trait::async_trait;

use libipld::{store::StoreParams, Block, Cid};
use std::error::Error as StdError;

struct DbBlockStore<T: types::Store> {
    db: T,
}

impl<T: types::Store> DbBlockStore<T> {
    fn get_bytes(&self, cid: &Cid) -> Result<Option<Vec<u8>>, Box<dyn StdError>> {
        Ok(self.db.read(cid.to_bytes())?)
    }
}
/// Wrapper for database to handle inserting and retrieving ipld data with Cids
pub trait BlockStore {
    type Params: StoreParams;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Box<dyn StdError>>;

    fn insert(&self, block: &Block<Self::Params>) -> Result<(), Box<dyn StdError>>;
}

impl<T: types::Store> BlockStore for DbBlockStore<T> {
    type Params = DefaultParams;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Box<dyn StdError>> {
        match self.get_bytes(cid)? {
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
    fn test_db_recovers_block() {
        let path = DBPath::new("write_test");
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
}
