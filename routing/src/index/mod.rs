mod bitfield;
mod error;
mod hash;
mod node;
mod pointer;
pub use self::{
    error::Error,
    hash::{BytesKey, Hash},
};
use crate::utils::ShrinkableMap;
use blockstore::types::BlockStore;
use libipld::Cid;
use node::Node;
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer};
use serde_cbor::to_vec;
use std::borrow::Borrow;
use std::sync::Arc;

const MAX_ARRAY_WIDTH: usize = 3;

/// Default bit width for indexing a hash at each depth level
const DEFAULT_BIT_WIDTH: u32 = 8;

type HashedKey = [u8; 32];

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct KeyValuePair<K, V>(K, V);

impl<K, V> KeyValuePair<K, V> {
    pub fn key(&self) -> &K {
        &self.0
    }
    pub fn value(&self) -> &V {
        &self.1
    }
}

impl<K, V> KeyValuePair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        KeyValuePair(key, value)
    }
}

/// Implementation of the HAMT data structure for Blockstore.
#[derive(Debug, Clone)]
pub struct Hamt<BS: BlockStore, V, K = BytesKey> {
    root: Node<K, V>,
    store: Arc<BS>,
    bit_width: u32,
}

impl<BS, V, K> Serialize for Hamt<BS, V, K>
where
    BS: BlockStore,
    K: Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.root.serialize(serializer)
    }
}

impl<'a, K: PartialEq, V: PartialEq, S: BlockStore> PartialEq for Hamt<S, V, K> {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
    }
}

impl<BS, V, K> Hamt<BS, V, K>
where
    K: Hash + Eq + PartialOrd + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    BS: BlockStore,
{
    pub fn new(store: Arc<BS>) -> Self {
        Self::new_with_bit_width(store, DEFAULT_BIT_WIDTH)
    }

    /// Construct hamt with a bit width
    pub fn new_with_bit_width(store: Arc<BS>, bit_width: u32) -> Self {
        Self {
            root: Node::default(),
            store,
            bit_width,
        }
    }

    /// Lazily instantiate a hamt from this root Cid.
    pub fn load(cid: &Cid, store: Arc<BS>) -> Result<Self, Error> {
        Self::load_with_bit_width(cid, store, DEFAULT_BIT_WIDTH)
    }

    /// Lazily instantiate a hamt from this root Cid with a specified bit width.
    pub fn load_with_bit_width(cid: &Cid, store: Arc<BS>, bit_width: u32) -> Result<Self, Error> {
        let root = Node::try_from(
            &dag_service::cat(store.clone(), *cid)
                .map_err(|_| Error::CidNotFound(cid.to_string()))?,
        )?;
        Ok(Self {
            root,
            store,
            bit_width,
        })
    }

    /// Returns a reference to the underlying store of the Hamt.
    pub fn store(&self) -> Arc<BS> {
        self.store.clone()
    }

    pub fn set(&mut self, key: K, value: V) -> Result<Option<V>, Error>
    where
        V: PartialEq,
    {
        self.root
            .set(key, value, self.store.clone(), self.bit_width, true)
            .map(|(r, _)| r)
    }

    pub fn extend<A>(&mut self, key: K, value: V) -> Result<bool, Error>
    where
        V: PartialEq + Extend<A> + IntoIterator<Item = A>,
    {
        self.root
            .extend(key, value, self.store.clone(), self.bit_width)
    }

    // if the values are themselves a map that can be reduced in size then delete a sub-value
    pub fn delete_subvalue<Q, A>(&mut self, key: &K, value: Q) -> Result<bool, Error>
    where
        V: PartialEq + ShrinkableMap<Q, A>,
    {
        self.root
            .shrink(key, value, self.store.clone(), self.bit_width)
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> Result<Option<&V>, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
        V: DeserializeOwned,
    {
        match self.root.get(k, self.store.clone(), self.bit_width)? {
            Some(v) => Ok(Some(v)),
            None => Ok(None),
        }
    }

    pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> Result<bool, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        Ok(self
            .root
            .get(k, self.store.clone(), self.bit_width)?
            .is_some())
    }

    pub fn delete<Q: ?Sized>(&mut self, k: &Q) -> Result<Option<(K, V)>, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.root
            .remove_entry(k, self.store.clone(), self.bit_width)
    }

    /// Returns true if the HAMT has no entries
    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }

    /// Flush root and return Cid for hamt
    pub fn flush(&mut self) -> Result<Cid, Error> {
        self.root.flush(self.store.clone())?;
        let data = to_vec(&self.root).map_err(|_| Error::InvalidNode)?;
        let cid = (dag_service::add(self.store.clone(), &data).map_err(Error::Other)?)
            .ok_or(Error::InvalidNode)?;

        Ok(cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockstore::memory::MemoryDB;
    use serde_bytes::ByteBuf;
    use std::fmt::Display;
    const BUCKET_SIZE: usize = 3;

    #[test]
    fn test_basics() {
        let store = Arc::new(MemoryDB::default());
        let mut hamt = Hamt::<_, String, _>::new(store.clone());
        hamt.set(1, "world".to_string()).unwrap();

        assert_eq!(hamt.get(&1).unwrap(), Some(&"world".to_string()));
        hamt.set(1, "world2".to_string()).unwrap();
        assert_eq!(hamt.get(&1).unwrap(), Some(&"world2".to_string()));
    }

    #[test]
    fn test_load() {
        let store = Arc::new(MemoryDB::default());

        let mut hamt: Hamt<_, _, usize> = Hamt::new(store.clone());
        hamt.set(1, "world".to_string()).unwrap();

        assert_eq!(hamt.get(&1).unwrap(), Some(&"world".to_string()));
        hamt.set(1, "world2".to_string()).unwrap();
        assert_eq!(hamt.get(&1).unwrap(), Some(&"world2".to_string()));
        let c = hamt.flush().unwrap();

        let new_hamt = Hamt::load(&c, store.clone()).unwrap();
        assert_eq!(hamt, new_hamt);

        // set value in the first one
        hamt.set(2, "stuff".to_string()).unwrap();

        // loading original hash should returnnot be equal now
        let new_hamt = Hamt::load(&c, store.clone()).unwrap();
        assert_ne!(hamt, new_hamt);

        // loading new hash
        let c2 = hamt.flush().unwrap();
        let new_hamt = Hamt::load(&c2, store.clone()).unwrap();
        assert_eq!(hamt, new_hamt);

        // loading from an empty store does not work
        let empty_store = Arc::new(MemoryDB::default());
        assert!(Hamt::<_, usize>::load(&c2, empty_store.clone()).is_err());

        // storing the hamt should produce the same cid as storing the root
        let c3 = hamt.flush().unwrap();
        assert_eq!(c3, c2);
    }

    #[test]
    fn set_with_no_effect_does_not_put() {
        let store = Arc::new(MemoryDB::default());

        let mut begn: Hamt<_, _> = Hamt::new_with_bit_width(store, 1);
        let entries = 2 * BUCKET_SIZE * 5;
        for i in 0..entries {
            begn.set(tstring(i), tstring("filler")).unwrap();
        }

        let c = begn.flush().unwrap();
        assert_eq!(
            c.to_string().as_str(),
            "bafybeic4huke27vu76hutuzucu542njmyy2xorhpilcamr3oa67s5u7r4q"
        );

        begn.set(tstring("favorite-animal"), tstring("bright green bear"))
            .unwrap();
        let c2 = begn.flush().unwrap();
        assert_eq!(
            c2.to_string().as_str(),
            "bafybeiesgc33l7oezlxerfriusfylg5frxb46qnnmwdtbhfxgupgodj7xm"
        );

        // This insert should not change value or affect reads or writes
        begn.set(tstring("favorite-animal"), tstring("bright green bear"))
            .unwrap();
        let c3 = begn.flush().unwrap();
        assert_eq!(
            c3.to_string().as_str(),
            "bafybeiesgc33l7oezlxerfriusfylg5frxb46qnnmwdtbhfxgupgodj7xm"
        );
    }

    #[test]
    fn delete() {
        let store = Arc::new(MemoryDB::default());

        let mut hamt: Hamt<_, _> = Hamt::new(store.clone());
        hamt.set(tstring("foo"), tstring("cat dog bear")).unwrap();
        hamt.set(tstring("bar"), tstring("cat dog")).unwrap();
        hamt.set(tstring("baz"), tstring("cat")).unwrap();

        let c = hamt.flush().unwrap();

        println!("{:?}", c.to_string().as_str());

        assert_eq!(
            c.to_string().as_str(),
            "bafybeiat7biujbauwcpypkplp3wptgiryvqcfica2fdobk7f3uy5bmk4vy"
        );

        let mut h2 = Hamt::<_, BytesKey>::load(&c, store.clone()).unwrap();
        assert!(h2.delete(&b"foo".to_vec()).unwrap().is_some());
        assert_eq!(h2.get(&b"foo".to_vec()).unwrap(), None);

        let c2 = h2.flush().unwrap();
        assert_eq!(
            c2.to_string().as_str(),
            "bafybeic4f6ujur2a4g7sfzzw4fyn5omgz7rgjrhar6mqfb74tyampnwdvy"
        );
    }

    #[test]
    fn delete_case() {
        let store = Arc::new(MemoryDB::default());

        let mut hamt: Hamt<_, _> = Hamt::new(store.clone());

        hamt.set([0].to_vec().into(), ByteBuf::from(b"Test data".as_ref()))
            .unwrap();

        let c = hamt.flush().unwrap();
        assert_eq!(
            c.to_string().as_str(),
            "bafybeidsjxd5mbj5y7mibv32zbjm2jbsdtjbuz6cnj2x5iaeincolnyb6a"
        );

        let mut h2 = Hamt::<_, ByteBuf>::load(&c, store.clone()).unwrap();
        assert!(h2.delete(&[0].to_vec()).unwrap().is_some());
        assert_eq!(h2.get(&[0].to_vec()).unwrap(), None);

        let c2 = h2.flush().unwrap();
        assert_eq!(
            c2.to_string().as_str(),
            "bafybeiccoeqxox7p7phl7yoixdf2zmdrbjuxtw2himz74gtm6twfzfhlym"
        );
    }

    #[test]
    fn set_delete_many() {
        let store = Arc::new(MemoryDB::default());
        // Test vectors setup specifically for bit width of 5
        let mut hamt: Hamt<_, BytesKey> = Hamt::new_with_bit_width(store, 5);

        for i in 0..200 {
            hamt.set(tstring(i), tstring(i)).unwrap();
        }

        let c1 = hamt.flush().unwrap();
        assert_eq!(
            c1.to_string().as_str(),
            "bafybeiak75oqku655ka4j667cz262mfq2cfuzzdftceolqdqtwtnxikgge"
        );

        for i in 200..400 {
            hamt.set(tstring(i), tstring(i)).unwrap();
        }

        let cid_all = hamt.flush().unwrap();
        assert_eq!(
            cid_all.to_string().as_str(),
            "bafybeiayw6jvlsequajuaroodavufrjebfloiwrdcejnlso2x7movhckwu"
        );

        for i in 200..400 {
            assert!(hamt.delete(&tstring(i)).unwrap().is_some());
        }
        // Ensure first 200 keys still exist
        for i in 0..200 {
            assert_eq!(hamt.get(&tstring(i)).unwrap(), Some(&tstring(i)));
        }

        let cid_d = hamt.flush().unwrap();
        assert_eq!(
            cid_d.to_string().as_str(),
            "bafybeiak75oqku655ka4j667cz262mfq2cfuzzdftceolqdqtwtnxikgge"
        );
    }

    #[test]
    fn clean_child_ordering() {
        let make_key = |i: u64| -> BytesKey {
            let mut key = unsigned_varint::encode::u64_buffer();
            let n = unsigned_varint::encode::u64(i, &mut key);
            n.to_vec().into()
        };

        let dummy_value: u8 = 42;

        let store = Arc::new(MemoryDB::default());

        let mut h: Hamt<_, _> = Hamt::new_with_bit_width(store.clone(), 5);

        for i in 100..195 {
            h.set(make_key(i), dummy_value).unwrap();
        }

        let root = h.flush().unwrap();
        assert_eq!(
            root.to_string().as_str(),
            "bafybeihreog7roba6isg3cxh3el2ieed3lhnflbod2ha3wcg5gmo25rrh4"
        );
        let mut h = Hamt::<_, u8>::load_with_bit_width(&root, store.clone(), 5).unwrap();

        h.delete(&make_key(104)).unwrap();
        h.delete(&make_key(108)).unwrap();
        let root = h.flush().unwrap();
        Hamt::<_, u8>::load_with_bit_width(&root, store.clone(), 5).unwrap();

        assert_eq!(
            root.to_string().as_str(),
            "bafybeigfzhom2yspl6ck3ciofwpkqzuihakkwcdwp2cyjta6x37epxk4xi"
        );
    }

    fn tstring(v: impl Display) -> BytesKey {
        BytesKey(v.to_string().into_bytes())
    }
}
