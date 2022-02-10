use crate::errors::Error;
use crate::types::{BlockStore, DBStore};
use libipld::DefaultParams;
use libipld::{Block, Cid};
pub use rocksdb::{perf::get_memory_usage_stats, Options, WriteBatch, DB};
use std::error::Error as StdError;
use std::path::Path;

#[derive(Debug)]
pub struct Db {
    pub db: DB,
}

impl Db {
    pub fn open<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.increase_parallelism(num_cpus::get() as i32);
        db_opts.set_write_buffer_size(256 * 1024 * 1024); // increase from 64MB to 256MB
        Ok(Self {
            db: DB::open(&mut db_opts, path)?,
        })
    }
}

impl DBStore for Db {
    fn total_size(&self) -> Result<usize, Error> {
        Ok(get_memory_usage_stats(Some(&[&self.db]), None)?.mem_table_total as usize)
    }

    fn write<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        Ok(self.db.put(key, value)?)
    }

    fn delete<K>(&self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        Ok(self.db.delete(key)?)
    }

    fn bulk_write<K, V>(&self, values: &[(K, V)]) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut batch = WriteBatch::default();
        for (k, v) in values {
            batch.put(k, v);
        }
        Ok(self.db.write(batch)?)
    }

    fn read<K>(&self, key: K) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]>,
    {
        self.db.get(key).map_err(Error::from)
    }

    fn exists<K>(&self, key: K) -> Result<bool, Error>
    where
        K: AsRef<[u8]>,
    {
        self.db
            .get_pinned(key)
            .map(|v| v.is_some())
            .map_err(Error::from)
    }
}

impl BlockStore for Db {
    type Params = DefaultParams;

    fn get(&mut self, cid: &Cid) -> Result<Block<Self::Params>, Box<dyn StdError + Send + Sync>> {
        let read_res = self.read(cid.to_bytes())?;
        match read_res {
            Some(bz) => Ok(Block::<Self::Params>::new(*cid, bz)?),
            None => Err(Box::new(Error::Other("Cid not in blockstore".to_string()))),
        }
    }
    fn insert(&mut self, block: &Block<Self::Params>) -> Result<(), Box<dyn StdError>> {
        let bytes = block.data();
        let cid = &block.cid().to_bytes();
        Ok(self.write(cid, bytes)?)
    }

    fn evict(&mut self, cid: &Cid) -> Result<(), Box<dyn StdError>> {
        Ok(self.delete(cid.to_bytes())?)
    }

    fn contains(&self, cid: &Cid) -> Result<bool, Box<dyn StdError>> {
        Ok(self.exists(cid.to_bytes())?)
    }
}

// --------------------------------------------------------------------------------------

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::test_helpers::*;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;
    use libipld::Block;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub struct DBPath {
        path: PathBuf,
    }

    impl DBPath {
        /// Suffixes the given `prefix` with a timestamp to ensure that subsequent test runs don't reuse
        /// an old database in case of panics prior to Drop being called.
        pub fn new(prefix: &str) -> DBPath {
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let path = format!(
                "{}.{}.{}",
                prefix,
                current_time.as_secs(),
                current_time.subsec_nanos()
            );

            DBPath {
                path: PathBuf::from(path),
            }
        }
    }

    impl Drop for DBPath {
        fn drop(&mut self) {
            let opts = Options::default();
            DB::destroy(&opts, &self.path).unwrap();
        }
    }

    impl AsRef<Path> for DBPath {
        fn as_ref(&self) -> &Path {
            &self.path
        }
    }

    #[test]
    fn db_write() {
        let path = DBPath::new("write_test");
        let mut db = Db::open(path.as_ref()).unwrap();
        test_write(&mut db);
    }

    #[test]
    fn db_read() {
        let path = DBPath::new("read_test");
        let mut db = Db::open(path.as_ref()).unwrap();
        test_read(&mut db);
    }

    #[test]
    fn db_exists() {
        let path = DBPath::new("exists_test");
        let mut db = Db::open(path.as_ref()).unwrap();
        test_exists(&mut db);
    }

    #[test]
    fn db_does_not_exist() {
        let path = DBPath::new("does_not_exists_test");
        let mut db = Db::open(path.as_ref()).unwrap();
        test_does_not_exist(&mut db);
    }

    #[test]
    fn db_delete() {
        let path = DBPath::new("delete_test");
        let mut db = Db::open(path.as_ref()).unwrap();
        test_delete(&mut db);
    }

    #[test]
    fn db_bulk_write() {
        let path = DBPath::new("bulk_write_test");
        let mut db = Db::open(path.as_ref()).unwrap();
        test_bulk_write(&mut db);
    }

    #[test]
    fn db_bulk_read() {
        let path = DBPath::new("bulk_read_test");
        let mut db = Db::open(path.as_ref()).unwrap();
        test_bulk_read(&mut db);
    }

    #[test]
    fn db_bulk_delete() {
        let path = DBPath::new("bulk_delete_test");
        let mut db = Db::open(path.as_ref()).unwrap();
        test_bulk_delete(&mut db);
    }

    #[test]
    fn test_db_recovers_block() {
        let path = DBPath::new("get_test");
        let mut store = Db::open(path.as_ref()).unwrap();

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
        let mut store = Db::open(path.as_ref()).unwrap();

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
