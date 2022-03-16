// Copyright 2019-2022 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

use super::bitfield::Bitfield;
use super::hash::HashBits;
use super::pointer::Pointer;
use super::{
    hash::{Hash, HashAlgorithm, Sha256},
    Error, KeyValuePair, MAX_ARRAY_WIDTH,
};
use blockstore::types::BlockStore;

use once_cell::unsync::OnceCell;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_cbor::{from_slice, to_vec};
use std::borrow::Borrow;
use std::fmt::Debug;
use std::sync::Arc;

/// Node in Hamt tree which contains bitfield of set indexes and pointers to nodes
#[derive(Debug)]
pub(crate) struct Node<K, V> {
    pub(crate) bitfield: Bitfield,
    pub(crate) pointers: Vec<Pointer<K, V>>,
}

impl<K, V> TryFrom<&Vec<u8>> for Node<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Error = Error;
    fn try_from(bytes: &Vec<u8>) -> Result<Self, Error> {
        let node = from_slice(bytes).map_err(|e| {
            println!("{:?}", e);
            Error::InvalidNode
        })?;
        Ok(node)
    }
}

impl<K: PartialEq, V: PartialEq> PartialEq for Node<K, V> {
    fn eq(&self, other: &Self) -> bool {
        (self.bitfield == other.bitfield) && (self.pointers == other.pointers)
    }
}

impl<K, V> Serialize for Node<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (&self.bitfield, &self.pointers).serialize(serializer)
    }
}

impl<'de, K, V> Deserialize<'de> for Node<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (bitfield, pointers) = Deserialize::deserialize(deserializer)?;
        Ok(Node { bitfield, pointers })
    }
}

impl<K, V> Default for Node<K, V> {
    fn default() -> Self {
        Node {
            bitfield: Bitfield::zero(),
            pointers: Vec::new(),
        }
    }
}

impl<K, V> Node<K, V>
where
    K: Hash + Eq + PartialOrd + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    pub fn set<S: BlockStore>(
        &mut self,
        key: K,
        value: V,
        store: Arc<S>,
        bit_width: u32,
        overwrite: bool,
    ) -> Result<(Option<V>, bool), Error>
    where
        V: PartialEq,
    {
        let hash = Sha256::hash(&key);
        self.modify_value(
            &mut HashBits::new(&hash),
            bit_width,
            0,
            key,
            value,
            store,
            overwrite,
        )
    }

    pub fn get<Q: ?Sized, S: BlockStore>(
        &self,
        k: &Q,
        store: Arc<S>,
        bit_width: u32,
    ) -> Result<Option<&V>, Error>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        Ok(self.search(k, store, bit_width)?.map(|kv| kv.value()))
    }

    pub fn remove_entry<Q: ?Sized, S>(
        &mut self,
        k: &Q,
        store: Arc<S>,
        bit_width: u32,
    ) -> Result<Option<(K, V)>, Error>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
        S: BlockStore,
    {
        let hash = Sha256::hash(k);
        self.rm_value(&mut HashBits::new(&hash), bit_width, 0, k, store)
    }

    pub fn is_empty(&self) -> bool {
        self.pointers.is_empty()
    }

    /// Search for a key.
    fn search<Q: ?Sized, S: BlockStore>(
        &self,
        q: &Q,
        store: Arc<S>,
        bit_width: u32,
    ) -> Result<Option<&KeyValuePair<K, V>>, Error>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = Sha256::hash(q);
        self.get_value(&mut HashBits::new(&hash), bit_width, 0, q, store)
    }

    fn get_value<Q: ?Sized, S: BlockStore>(
        &self,
        hashed_key: &mut HashBits,
        bit_width: u32,
        depth: usize,
        key: &Q,
        store: Arc<S>,
    ) -> Result<Option<&KeyValuePair<K, V>>, Error>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let idx = hashed_key.next(bit_width)?;

        if !self.bitfield.test_bit(idx) {
            return Ok(None);
        }

        let cindex = self.index_for_bit_pos(idx);
        let child = self.get_child(cindex);
        match child {
            Pointer::Link { cid, cache } => {
                if let Some(cached_node) = cache.get() {
                    // Link node is cached
                    cached_node.get_value(hashed_key, bit_width, depth + 1, key, store)
                } else {
                    let node = Box::new(Node::try_from(
                        &dag_service::cat(store.clone(), *cid)
                            .map_err(|_| Error::CidNotFound(cid.to_string()))?,
                    )?);

                    // Intentionally ignoring error, cache will always be the same.
                    let cache_node = cache.get_or_init(|| node);
                    cache_node.get_value(hashed_key, bit_width, depth + 1, key, store)
                }
            }
            Pointer::Dirty(n) => n.get_value(hashed_key, bit_width, depth + 1, key, store),
            Pointer::Values(vals) => Ok(vals.iter().find(|kv| key.eq(kv.key().borrow()))),
        }
    }

    /// Internal method to modify values.
    fn modify_value<S: BlockStore>(
        &mut self,
        hashed_key: &mut HashBits,
        bit_width: u32,
        depth: usize,
        key: K,
        value: V,
        store: Arc<S>,
        overwrite: bool,
    ) -> Result<(Option<V>, bool), Error>
    where
        V: PartialEq,
    {
        let idx = hashed_key.next(bit_width)?;

        // No existing values at this point.
        if !self.bitfield.test_bit(idx) {
            self.insert_child(idx, key, value);
            return Ok((None, true));
        }

        let cindex = self.index_for_bit_pos(idx);
        let child = self.get_child_mut(cindex);

        match child {
            Pointer::Link { cid, cache } => {
                let res = || -> Result<Box<Node<K, V>>, Error> {
                    let node = Box::new(Node::try_from(
                        &dag_service::cat(store.clone(), *cid)
                            .map_err(|_| Error::CidNotFound(cid.to_string()))?,
                    )?);
                    Ok(node)
                };
                cache.get_or_try_init(res)?;
                let child_node = cache.get_mut().expect("filled line above");

                let (old, modified) = child_node.modify_value(
                    hashed_key,
                    bit_width,
                    depth + 1,
                    key,
                    value,
                    store,
                    overwrite,
                )?;
                if modified {
                    *child = Pointer::Dirty(std::mem::take(child_node));
                }
                Ok((old, modified))
            }
            Pointer::Dirty(n) => Ok(n.modify_value(
                hashed_key,
                bit_width,
                depth + 1,
                key,
                value,
                store,
                overwrite,
            )?),
            Pointer::Values(vals) => {
                // Update, if the key already exists.
                if let Some(i) = vals.iter().position(|p| p.key() == &key) {
                    if overwrite {
                        let value_changed = vals[i].value() != &value;
                        return Ok((
                            Some(std::mem::replace(&mut vals[i].1, value)),
                            value_changed,
                        ));
                    } else {
                        // Can't overwrite, return None and false that the Node was not modified.
                        return Ok((None, false));
                    }
                }

                // If the array is full, create a subshard and insert everything
                if vals.len() >= MAX_ARRAY_WIDTH {
                    let mut sub = Node::<K, V>::default();
                    let consumed = hashed_key.consumed;
                    let modified = sub.modify_value(
                        hashed_key,
                        bit_width,
                        depth + 1,
                        key,
                        value,
                        store.clone(),
                        overwrite,
                    )?;
                    let kvs = std::mem::take(vals);
                    for p in kvs.into_iter() {
                        let hash = Sha256::hash(p.key());
                        sub.modify_value(
                            &mut HashBits::new_at_index(&hash, consumed),
                            bit_width,
                            depth + 1,
                            p.0,
                            p.1,
                            store.clone(),
                            overwrite,
                        )?;
                    }

                    *child = Pointer::Dirty(Box::new(sub));

                    return Ok(modified);
                }

                // Otherwise insert the element into the array in order.
                let max = vals.len();
                let idx = vals.iter().position(|c| c.key() > &key).unwrap_or(max);

                let np = KeyValuePair::new(key, value);
                vals.insert(idx, np);

                Ok((None, true))
            }
        }
    }

    /// Internal method to delete entries.
    fn rm_value<Q: ?Sized, S: BlockStore>(
        &mut self,
        hashed_key: &mut HashBits,
        bit_width: u32,
        depth: usize,
        key: &Q,
        store: Arc<S>,
    ) -> Result<Option<(K, V)>, Error>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let idx = hashed_key.next(bit_width)?;

        // No existing values at this point.
        if !self.bitfield.test_bit(idx) {
            return Ok(None);
        }

        let cindex = self.index_for_bit_pos(idx);
        let child = self.get_child_mut(cindex);

        match child {
            Pointer::Link { cid, cache } => {
                let res = || -> Result<Box<Node<K, V>>, Error> {
                    let node = Box::new(Node::try_from(
                        &dag_service::cat(store.clone(), *cid)
                            .map_err(|_| Error::CidNotFound(cid.to_string()))?,
                    )?);
                    Ok(node)
                };
                cache.get_or_try_init(res)?;
                let child_node = cache.get_mut().expect("filled line above");

                let deleted = child_node.rm_value(hashed_key, bit_width, depth + 1, key, store)?;
                if deleted.is_some() {
                    *child = Pointer::Dirty(std::mem::take(child_node));

                    // Clean to retrieve canonical form
                    child.clean()?;
                }

                Ok(deleted)
            }
            Pointer::Dirty(n) => {
                // Delete value and return deleted value
                let deleted = n.rm_value(hashed_key, bit_width, depth + 1, key, store)?;

                // Clean to ensure canonical form
                child.clean()?;
                Ok(deleted)
            }
            Pointer::Values(vals) => {
                // Delete value
                for (i, p) in vals.iter().enumerate() {
                    if key.eq(p.key().borrow()) {
                        let old = if vals.len() == 1 {
                            if let Pointer::Values(new_v) = self.rm_child(cindex, idx) {
                                new_v.into_iter().next().unwrap()
                            } else {
                                unreachable!()
                            }
                        } else {
                            vals.remove(i)
                        };
                        return Ok(Some((old.0, old.1)));
                    }
                }

                Ok(None)
            }
        }
    }

    pub fn flush<S: BlockStore>(&mut self, store: Arc<S>) -> Result<(), Error> {
        for pointer in &mut self.pointers {
            if let Pointer::Dirty(node) = pointer {
                // Flush cached sub node to clear it's cache
                node.flush(store.clone())?;

                let data = to_vec(node).map_err(|_| Error::InvalidNode)?;

                let cid = (dag_service::add(store.clone(), &data).map_err(|e| Error::Other(e))?)
                    .ok_or(Error::InvalidNode)?;

                // Can keep the flushed node in link cache
                let cache = OnceCell::from(std::mem::take(node));

                // Replace cached node with Cid link
                *pointer = Pointer::Link { cid, cache };
            }
        }

        Ok(())
    }

    fn rm_child(&mut self, i: usize, idx: u32) -> Pointer<K, V> {
        self.bitfield.clear_bit(idx);
        self.pointers.remove(i)
    }

    fn insert_child(&mut self, idx: u32, key: K, value: V) {
        let i = self.index_for_bit_pos(idx);
        self.bitfield.set_bit(idx);
        self.pointers
            .insert(i as usize, Pointer::from_key_value(key, value))
    }

    fn index_for_bit_pos(&self, bp: u32) -> usize {
        let mask = Bitfield::zero().set_bits_le(bp);
        assert_eq!(mask.count_ones(), bp as usize);
        mask.and(&self.bitfield).count_ones()
    }

    fn get_child_mut(&mut self, i: usize) -> &mut Pointer<K, V> {
        &mut self.pointers[i]
    }

    fn get_child(&self, i: usize) -> &Pointer<K, V> {
        &self.pointers[i]
    }
}
