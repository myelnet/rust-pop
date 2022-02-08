use crate::errors::Error;
use crate::types::{BlockStore, DBStore};
use libipld::DefaultParams;
use parking_lot::RwLock;
pub use rocksdb::{Options, WriteBatch, DB};
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
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
            db: DB::open(&db_opts, path)?,
        })
    }
}

impl DBStore for Db {
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
}

// --------------------------------------------------------------------------------------

/// A thread-safe `HashMap` wrapper.
#[derive(Debug, Default)]
pub struct MemoryDB {
    db: RwLock<HashMap<u64, Vec<u8>>>,
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
        }
    }
}

impl DBStore for MemoryDB {
    fn write<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db
            .write()
            .insert(Self::db_index(key), value.as_ref().to_vec());
        Ok(())
    }

    fn delete<K>(&self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
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
}

#[cfg(test)]
pub mod tests {

    use super::*;
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

    pub fn test_write<DB>(db: &DB)
    where
        DB: DBStore,
    {
        let key = [1];
        let value = [1];
        db.write(key, value).unwrap();
    }

    pub fn test_read<DB>(db: &DB)
    where
        DB: DBStore,
    {
        let key = [0];
        let value = [1];
        db.write(key, value).unwrap();
        let res = db.read(key).unwrap().unwrap();
        assert_eq!(value.as_ref(), res.as_slice());
    }

    pub fn test_exists<DB>(db: &DB)
    where
        DB: DBStore,
    {
        let key = [0];
        let value = [1];
        db.write(key, value).unwrap();
        let res = db.exists(key).unwrap();
        assert_eq!(res, true);
    }

    pub fn test_does_not_exist<DB>(db: &DB)
    where
        DB: DBStore,
    {
        let key = [0];
        let res = db.exists(key).unwrap();
        assert_eq!(res, false);
    }

    pub fn test_delete<DB>(db: &DB)
    where
        DB: DBStore,
    {
        let key = [0];
        let value = [1];
        db.write(key, value).unwrap();
        let res = db.exists(key).unwrap();
        assert_eq!(res, true);
        db.delete(key).unwrap();
        let res = db.exists(key).unwrap();
        assert_eq!(res, false);
    }

    pub fn test_bulk_write<DB>(db: &DB)
    where
        DB: DBStore,
    {
        let values = [([0], [0]), ([1], [1]), ([2], [2])];
        db.bulk_write(&values).unwrap();
        for (k, _) in values.iter() {
            let res = db.exists(*k).unwrap();
            assert_eq!(res, true);
        }
    }

    pub fn test_bulk_read<DB>(db: &DB)
    where
        DB: DBStore,
    {
        let keys = [[0], [1], [2]];
        let values = [[0], [1], [2]];
        let kvs: Vec<_> = keys.iter().zip(values.iter()).collect();
        db.bulk_write(&kvs).unwrap();
        let results = db.bulk_read(&keys).unwrap();
        for (result, value) in results.iter().zip(values.iter()) {
            match result {
                Some(v) => assert_eq!(v, value),
                None => panic!("No values found!"),
            }
        }
    }

    pub fn test_bulk_delete<DB>(db: &DB)
    where
        DB: DBStore,
    {
        let keys = [[0], [1], [2]];
        let values = [[0], [1], [2]];
        let kvs: Vec<_> = keys.iter().zip(values.iter()).collect();
        db.bulk_write(&kvs).unwrap();
        db.bulk_delete(&keys).unwrap();
        for k in keys.iter() {
            let res = db.exists(*k).unwrap();
            assert_eq!(res, false);
        }
    }

    #[test]
    fn db_write() {
        let path = DBPath::new("write_test");
        let db = Db::open(path.as_ref()).unwrap();
        test_write(&db);
    }

    #[test]
    fn db_read() {
        let path = DBPath::new("read_test");
        let db = Db::open(path.as_ref()).unwrap();
        test_read(&db);
    }

    #[test]
    fn db_exists() {
        let path = DBPath::new("exists_test");
        let db = Db::open(path.as_ref()).unwrap();
        test_exists(&db);
    }

    #[test]
    fn db_does_not_exist() {
        let path = DBPath::new("does_not_exists_test");
        let db = Db::open(path.as_ref()).unwrap();
        test_does_not_exist(&db);
    }

    #[test]
    fn db_delete() {
        let path = DBPath::new("delete_test");
        let db = Db::open(path.as_ref()).unwrap();
        test_delete(&db);
    }

    #[test]
    fn db_bulk_write() {
        let path = DBPath::new("bulk_write_test");
        let db = Db::open(path.as_ref()).unwrap();
        test_bulk_write(&db);
    }

    #[test]
    fn db_bulk_read() {
        let path = DBPath::new("bulk_read_test");
        let db = Db::open(path.as_ref()).unwrap();
        test_bulk_read(&db);
    }

    #[test]
    fn db_bulk_delete() {
        let path = DBPath::new("bulk_delete_test");
        let db = Db::open(path.as_ref()).unwrap();
        test_bulk_delete(&db);
    }

    #[test]
    fn mem_db_write() {
        let db = MemoryDB::default();
        test_write(&db);
    }

    #[test]
    fn mem_db_read() {
        let db = MemoryDB::default();
        test_read(&db);
    }

    #[test]
    fn mem_db_exists() {
        let db = MemoryDB::default();
        test_exists(&db);
    }

    #[test]
    fn mem_db_does_not_exist() {
        let db = MemoryDB::default();
        test_does_not_exist(&db);
    }

    #[test]
    fn mem_db_delete() {
        let db = MemoryDB::default();
        test_delete(&db);
    }

    #[test]
    fn mem_db_bulk_write() {
        let db = MemoryDB::default();
        test_bulk_write(&db);
    }

    #[test]
    fn mem_db_bulk_read() {
        let db = MemoryDB::default();
        test_bulk_read(&db);
    }

    #[test]
    fn mem_db_bulk_delete() {
        let db = MemoryDB::default();
        test_bulk_delete(&db);
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
    fn test_db_recovers_block() {
        let path = DBPath::new("get_test");
        let store = Db::open(path.as_ref()).unwrap();

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
        let store = Db::open(path.as_ref()).unwrap();

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
