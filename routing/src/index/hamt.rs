// Copyright 2019-2022 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

use crate::index::node::Node;
use crate::index::{
    hash::{BytesKey, Hash},
    Error, HashAlgorithm, Sha256, DEFAULT_BIT_WIDTH,
};
use blockstore::types::BlockStore;
use libipld::Cid;
use serde::{de::DeserializeOwned, Serialize, Serializer};
use serde_cbor::{from_slice, to_vec};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::borrow::Borrow;
use std::error::Error as StdError;
use std::marker::PhantomData;
use std::sync::Arc;

/// Implementation of the HAMT data structure for IPLD.
///
/// # Examples
///
/// ```
/// use ipld_hamt::Hamt;
///
/// let store = db::MemoryDB::default();
///
/// let mut map: Hamt<_, _, usize> = Hamt::new(store.clone());
/// map.set(1, "a".to_string()).unwrap();
/// assert_eq!(map.get(&1).unwrap(), Some(&"a".to_string()));
/// assert_eq!(map.delete(&1).unwrap(), Some((1, "a".to_string())));
/// assert_eq!(map.get::<_>(&1).unwrap(), None);
/// let cid = map.flush().unwrap();
/// ```
#[derive(Debug)]
pub struct Hamt<BS: BlockStore, V, K = BytesKey, H = Sha256> {
    root: Node<K, V, H>,
    store: Arc<BS>,

    bit_width: u32,
    hash: PhantomData<H>,
}

impl<BS, V, K, H> Serialize for Hamt<BS, V, K, H>
where
    BS: BlockStore,
    K: Serialize,
    V: Serialize,
    H: HashAlgorithm,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.root.serialize(serializer)
    }
}

impl<'a, K: PartialEq, V: PartialEq, S: BlockStore, H: HashAlgorithm> PartialEq
    for Hamt<S, V, K, H>
{
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
    }
}

impl<BS, V, K, H> Hamt<BS, V, K, H>
where
    K: Hash + Eq + PartialOrd + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    BS: BlockStore,
    H: HashAlgorithm,
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
            hash: Default::default(),
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
            hash: Default::default(),
        })
    }

    /// Sets the root based on the Cid of the root node using the Hamt store
    pub fn set_root(&mut self, cid: &Cid) -> Result<(), Error> {
        let root = Node::try_from(
            &dag_service::cat(self.store.clone(), *cid)
                .map_err(|_| Error::CidNotFound(cid.to_string()))?,
        )?;

        self.root = root;

        Ok(())
    }

    /// Returns a reference to the underlying store of the Hamt.
    pub fn store(&self) -> Arc<BS> {
        self.store.clone()
    }

    /// Inserts a key-value pair into the HAMT.
    ///
    /// If the HAMT did not have this key present, `None` is returned.
    ///
    /// If the HAMT did have this key present, the value is updated, and the old
    /// value is returned. The key is not updated, though;
    ///
    /// # Examples
    ///
    /// ```
    /// use ipld_hamt::Hamt;
    ///
    /// let store = db::MemoryDB::default();
    ///
    /// let mut map: Hamt<_, _, usize> = Hamt::new(store.clone());
    /// map.set(37, "a".to_string()).unwrap();
    /// assert_eq!(map.is_empty(), false);
    ///
    /// map.set(37, "b".to_string()).unwrap();
    /// map.set(37, "c".to_string()).unwrap();
    /// ```
    pub fn set(&mut self, key: K, value: V) -> Result<Option<V>, Error>
    where
        V: PartialEq,
    {
        self.root
            .set(key, value, self.store.clone(), self.bit_width, true)
            .map(|(r, _)| r)
    }

    /// Inserts a key-value pair into the HAMT only if that key does not already exist.
    ///
    /// If the HAMT did not have this key present, `true` is returned and the key/value is added.
    ///
    /// If the HAMT did have this key present, this function will return false
    ///
    /// # Examples
    ///
    /// ```
    /// use ipld_hamt::Hamt;
    ///
    /// let store = db::MemoryDB::default();
    ///
    /// let mut map: Hamt<_, _, usize> = Hamt::new(store.clone());
    /// let a = map.set_if_absent(37, "a".to_string()).unwrap();
    /// assert_eq!(map.is_empty(), false);
    /// assert_eq!(a, true);
    ///
    /// let b = map.set_if_absent(37, "b".to_string()).unwrap();
    /// assert_eq!(b, false);
    /// assert_eq!(map.get(&37).unwrap(), Some(&"a".to_string()));
    ///
    /// let c = map.set_if_absent(30, "c".to_string()).unwrap();
    /// assert_eq!(c, true);
    /// ```
    pub fn set_if_absent(&mut self, key: K, value: V) -> Result<bool, Error>
    where
        V: PartialEq,
    {
        self.root
            .set(key, value, self.store.clone(), self.bit_width, false)
            .map(|(_, set)| set)
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// `Hash` and `Eq` on the borrowed form *must* match those for
    /// the key type.
    ///
    /// # Examples
    ///
    /// ```
    /// use ipld_hamt::Hamt;
    ///
    /// let store = db::MemoryDB::default();
    ///
    /// let mut map: Hamt<_, _, usize> = Hamt::new(store.clone());
    /// map.set(1, "a".to_string()).unwrap();
    /// assert_eq!(map.get(&1).unwrap(), Some(&"a".to_string()));
    /// assert_eq!(map.get(&2).unwrap(), None);
    /// ```
    #[inline]
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

    /// Returns `true` if a value exists for the given key in the HAMT.
    ///
    /// The key may be any borrowed form of the map's key type, but
    /// `Hash` and `Eq` on the borrowed form *must* match those for
    /// the key type.
    ///
    /// # Examples
    ///
    /// ```
    /// use ipld_hamt::Hamt;
    ///
    /// let store = db::MemoryDB::default();
    ///
    /// let mut map: Hamt<_, _, usize> = Hamt::new(store.clone());
    /// map.set(1, "a".to_string()).unwrap();
    /// assert_eq!(map.contains_key(&1).unwrap(), true);
    /// assert_eq!(map.contains_key(&2).unwrap(), false);
    /// ```
    #[inline]
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

    /// Removes a key from the HAMT, returning the value at the key if the key
    /// was previously in the HAMT.
    ///
    /// The key may be any borrowed form of the HAMT's key type, but
    /// `Hash` and `Eq` on the borrowed form *must* match those for
    /// the key type.
    ///
    /// # Examples
    ///
    /// ```
    /// use ipld_hamt::Hamt;
    ///
    /// let store = db::MemoryDB::default();
    ///
    /// let mut map: Hamt<_, _, usize> = Hamt::new(store.clone());
    /// map.set(1, "a".to_string()).unwrap();
    /// assert_eq!(map.delete(&1).unwrap(), Some((1, "a".to_string())));
    /// assert_eq!(map.delete(&1).unwrap(), None);
    /// ```
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
        let cid = (dag_service::add(self.store.clone(), &data).map_err(|e| Error::Other(e))?)
            .ok_or(Error::InvalidNode)?;

        Ok(cid)
    }

    /// Iterates over each KV in the Hamt and runs a function on the values.
    ///
    /// This function will constrain all values to be of the same type
    ///
    /// # Examples
    ///
    /// ```
    /// use ipld_hamt::Hamt;
    ///
    /// let store = db::MemoryDB::default();
    ///
    /// let mut map: Hamt<_, _, usize> = Hamt::new(store.clone());
    /// map.set(1, 1).unwrap();
    /// map.set(4, 2).unwrap();
    ///
    /// let mut total = 0;
    /// map.for_each(|_, v: &u64| {
    ///    total += v;
    ///    Ok(())
    /// }).unwrap();
    /// assert_eq!(total, 3);
    /// ```
    #[inline]
    pub fn for_each<F>(&self, mut f: F) -> Result<(), Box<dyn StdError>>
    where
        V: DeserializeOwned,
        F: FnMut(&K, &V) -> Result<(), Box<dyn StdError>>,
    {
        self.root.for_each(self.store.clone(), &mut f)
    }
}

#[cfg(test)]
mod tests {
    // Copyright 2019-2022 ChainSafe Systems
    // SPDX-License-Identifier: Apache-2.0, MIT
    use super::*;
    use blockstore::memory::MemoryDB;
    use libipld::multihash::Code::Blake2b256;
    use serde_bytes::ByteBuf;
    use std::fmt::Display;

    // Redeclaring max array size of Hamt to avoid exposing value
    const BUCKET_SIZE: usize = 3;

    #[cfg(feature = "identity")]
    use ipld_hamt::Identity;

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
    fn test_set_if_absent() {
        let store = Arc::new(MemoryDB::default());

        let mut hamt: Hamt<_, _> = Hamt::new(store.clone());
        assert!(hamt
            .set_if_absent(tstring("favorite-animal"), tstring("owl bear"))
            .unwrap());

        // Next two are negatively asserted, shouldn't change
        assert!(!hamt
            .set_if_absent(tstring("favorite-animal"), tstring("bright green bear"))
            .unwrap());
        assert!(!hamt
            .set_if_absent(tstring("favorite-animal"), tstring("owl bear"))
            .unwrap());

        let c = hamt.flush().unwrap();

        let mut h2 = Hamt::<_, BytesKey>::load(&c, store.clone()).unwrap();
        // Reloading should still have same effect
        assert!(!h2
            .set_if_absent(tstring("favorite-animal"), tstring("bright green bear"))
            .unwrap());

        assert_eq!(
            c.to_string().as_str(),
            "bafy2bzaced2tgnlsq4n2ioe6ldy75fw3vlrrkyfv4bq6didbwoob2552zvpuk"
        );
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
            "bafy2bzacebjilcrsqa4uyxuh36gllup4rlgnvwgeywdm5yqq2ks4jrsj756qq"
        );

        begn.set(tstring("favorite-animal"), tstring("bright green bear"))
            .unwrap();
        let c2 = begn.flush().unwrap();
        assert_eq!(
            c2.to_string().as_str(),
            "bafy2bzacea7biyabzk7v7le2rrlec5tesjbdnymh5sk4lfprxibg4rtudwtku"
        );

        // This insert should not change value or affect reads or writes
        begn.set(tstring("favorite-animal"), tstring("bright green bear"))
            .unwrap();
        let c3 = begn.flush().unwrap();
        assert_eq!(
            c3.to_string().as_str(),
            "bafy2bzacea7biyabzk7v7le2rrlec5tesjbdnymh5sk4lfprxibg4rtudwtku"
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
            "bafy2bzaceb2hikcc6tfuuuuehjstbiq356oruwx6ejyse77zupq445unranv6"
        );

        let mut h2 = Hamt::<_, ByteBuf>::load(&c, store.clone()).unwrap();
        assert!(h2.delete(&[0].to_vec()).unwrap().is_some());
        assert_eq!(h2.get(&[0].to_vec()).unwrap(), None);

        let c2 = h2.flush().unwrap();
        assert_eq!(
            c2.to_string().as_str(),
            "bafy2bzaceamp42wmmgr2g2ymg46euououzfyck7szknvfacqscohrvaikwfay"
        );
    }

    // #[test]
    // fn reload_empty() {
    //     let store = Arc::new(MemoryDB::default());
    //
    //     let hamt: Hamt<_, ()> = Hamt::new(store.clone());
    //     let c = store.put(&hamt, Blake2b256).unwrap();
    //
    //     let h2 = Hamt::<_, ()>::load(&c, store.clone()).unwrap();
    //     let c2 = store.put(&h2, Blake2b256).unwrap();
    //     assert_eq!(c, c2);
    //     assert_eq!(
    //         c.to_string().as_str(),
    //         "bafy2bzaceamp42wmmgr2g2ymg46euououzfyck7szknvfacqscohrvaikwfay"
    //     );
    // }

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
            "bafy2bzaceczhz54xmmz3xqnbmvxfbaty3qprr6dq7xh5vzwqbirlsnbd36z7a"
        );

        for i in 200..400 {
            hamt.set(tstring(i), tstring(i)).unwrap();
        }

        let cid_all = hamt.flush().unwrap();
        assert_eq!(
            cid_all.to_string().as_str(),
            "bafy2bzacecxcp736xkl2mcyjlors3tug6vdlbispbzxvb75xlrhthiw2xwxvw"
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
            "bafy2bzaceczhz54xmmz3xqnbmvxfbaty3qprr6dq7xh5vzwqbirlsnbd36z7a"
        );
    }
    #[test]
    fn for_each() {
        let store = Arc::new(MemoryDB::default());

        let mut hamt: Hamt<_, BytesKey> = Hamt::new_with_bit_width(store.clone(), 5);

        for i in 0..200 {
            hamt.set(tstring(i), tstring(i)).unwrap();
        }

        // Iterating through hamt with dirty caches.
        let mut count = 0;
        hamt.for_each(|k, v| {
            assert_eq!(k, v);
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 200);

        let c = hamt.flush().unwrap();
        assert_eq!(
            c.to_string().as_str(),
            "bafy2bzaceczhz54xmmz3xqnbmvxfbaty3qprr6dq7xh5vzwqbirlsnbd36z7a"
        );

        let mut hamt: Hamt<_, BytesKey> = Hamt::load_with_bit_width(&c, store.clone(), 5).unwrap();

        // Iterating through hamt with no cache.
        let mut count = 0;
        hamt.for_each(|k, v| {
            assert_eq!(k, v);
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 200);

        // Iterating through hamt with cached nodes.
        let mut count = 0;
        hamt.for_each(|k, v| {
            assert_eq!(k, v);
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 200);

        let c = hamt.flush().unwrap();
        assert_eq!(
            c.to_string().as_str(),
            "bafy2bzaceczhz54xmmz3xqnbmvxfbaty3qprr6dq7xh5vzwqbirlsnbd36z7a"
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
            "bafy2bzacebqox3gtng4ytexyacr6zmaliyins3llnhbnfbcrqmhzuhmuuawqk"
        );
        let mut h = Hamt::<_, u8>::load_with_bit_width(&root, store.clone(), 5).unwrap();

        h.delete(&make_key(104)).unwrap();
        h.delete(&make_key(108)).unwrap();
        let root = h.flush().unwrap();
        Hamt::<_, u8>::load_with_bit_width(&root, store.clone(), 5).unwrap();

        assert_eq!(
            root.to_string().as_str(),
            "bafy2bzacedlyeuub3mo4aweqs7zyxrbldsq2u4a2taswubudgupglu2j4eru6"
        );
    }

    fn tstring(v: impl Display) -> BytesKey {
        BytesKey(v.to_string().into_bytes())
    }
}
