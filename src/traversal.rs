use crate::empty_map;
use async_recursion::async_recursion;
use async_std::channel::{bounded, Receiver, Sender};
use async_trait::async_trait;
use fnv::FnvHashMap;
use libipld::cid::Error as CidError;
use libipld::codec::Decode;
use libipld::ipld::{Ipld, IpldIndex};
use libipld::multihash::Error as MultihashError;
use libipld::store::{Store, StoreParams};
use libipld::{Block, Cid};
use protobuf::ProtobufError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_cbor::{error::Error as CborError, from_slice, to_vec};
use std::fmt;
use std::io::Error as StdError;
use std::sync::Arc;
use thiserror::Error;
use Selector::*;

pub trait Cbor: Serialize + DeserializeOwned {
    fn marshal_cbor(&self) -> Result<Vec<u8>, Error> {
        Ok(to_vec(&self)?)
    }

    fn unmarshal_cbor(bz: &[u8]) -> Result<Self, Error> {
        Ok(from_slice(bz)?)
    }
}

/// Represents either a key in a map or an index in a list.
#[derive(Clone, Debug, PartialEq)]
pub enum PathSegment {
    /// Key in a map
    String(String),
    /// Index in a list
    Int(usize),
}

impl PathSegment {
    pub fn ipld_index<'a>(&self) -> IpldIndex<'a> {
        match self {
            PathSegment::String(string) => IpldIndex::Map(string.clone()),
            PathSegment::Int(int) => IpldIndex::List(int.clone()),
        }
    }
}

impl From<usize> for PathSegment {
    fn from(i: usize) -> Self {
        Self::Int(i)
    }
}

impl From<String> for PathSegment {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for PathSegment {
    fn from(s: &str) -> Self {
        match s.parse::<usize>() {
            Ok(u) => PathSegment::Int(u),
            Err(_) => PathSegment::String(s.to_owned()),
        }
    }
}

impl fmt::Display for PathSegment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PathSegment::String(s) => write!(f, "{}", s),
            PathSegment::Int(i) => write!(f, "{}", i),
        }
    }
}

#[derive(Debug, PartialEq, Default, Clone)]
pub struct Path {
    segments: Vec<PathSegment>,
}

impl Path {
    pub fn new(segments: Vec<PathSegment>) -> Self {
        Self { segments }
    }
    pub fn push(&mut self, seg: PathSegment) {
        self.segments.push(seg)
    }
    pub fn pop(&mut self) -> Option<PathSegment> {
        self.segments.pop()
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.segments.is_empty() {
            return Ok(());
        }

        write!(f, "{}", self.segments[0])?;
        for v in &self.segments[1..] {
            write!(f, "/{}", v)?;
        }

        Ok(())
    }
}

impl From<&str> for Path {
    fn from(s: &str) -> Self {
        let segments: Vec<PathSegment> = s
            .split('/')
            .filter(|s| !s.is_empty())
            .map(PathSegment::from)
            .collect();
        Self { segments }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Selector {
    #[serde(rename = "a")]
    ExploreAll {
        #[serde(rename = ">")]
        next: Box<Selector>,
    },
    #[serde(rename = "R")]
    ExploreRecursive {
        #[serde(rename = "l")]
        limit: RecursionLimit,
        #[serde(rename = ":>")]
        sequence: Box<Selector>,
        #[serde(skip_deserializing)]
        /// Used to index current
        current: Option<Box<Selector>>,
    },
    #[serde(rename = "@", with = "empty_map")]
    ExploreRecursiveEdge,
}

impl Cbor for Selector {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Copy)]
pub enum RecursionLimit {
    #[serde(rename = "none", with = "empty_map")]
    None,
    #[serde(rename = "depth")]
    Depth(u64),
}

impl Selector {
    pub fn interests(&self) -> Option<Vec<PathSegment>> {
        match self {
            ExploreAll { .. } => None,
            ExploreRecursive {
                current, sequence, ..
            } => {
                if let Some(selector) = current {
                    selector.interests()
                } else {
                    sequence.interests()
                }
            }
            ExploreRecursiveEdge => {
                // Should never be called on this variant
                Some(vec![])
            }
        }
    }
    pub fn explore(self, ipld: &Ipld, p: &PathSegment) -> Option<Selector> {
        match self {
            ExploreAll { next } => Some(*next),
            ExploreRecursive {
                current,
                sequence,
                limit,
            } => {
                let next = current
                    .unwrap_or_else(|| sequence.clone())
                    .explore(ipld, p)?;

                if !has_recursive_edge(&next) {
                    return Some(ExploreRecursive {
                        sequence,
                        current: Some(next.into()),
                        limit,
                    });
                }

                match limit {
                    RecursionLimit::Depth(depth) => {
                        if depth < 2 {
                            return replace_recursive_edge(next, None);
                        }
                        return Some(ExploreRecursive {
                            current: replace_recursive_edge(next, Some(*sequence.clone()))
                                .map(Box::new),
                            sequence,
                            limit: RecursionLimit::Depth(depth - 1),
                        });
                    }
                    RecursionLimit::None => Some(ExploreRecursive {
                        current: replace_recursive_edge(next, Some(*sequence.clone()))
                            .map(Box::new),
                        sequence,
                        limit,
                    }),
                }
            }
            ExploreRecursiveEdge => None,
        }
    }
    pub fn decide(&self) -> bool {
        match self {
            ExploreRecursive {
                current, sequence, ..
            } => {
                if let Some(curr) = current {
                    curr.decide()
                } else {
                    sequence.decide()
                }
            }
            _ => false,
        }
    }
}

fn has_recursive_edge(next_sel: &Selector) -> bool {
    match next_sel {
        ExploreRecursiveEdge { .. } => true,
        _ => false,
    }
}

fn replace_recursive_edge(next_sel: Selector, replace: Option<Selector>) -> Option<Selector> {
    match next_sel {
        ExploreRecursiveEdge => replace,
        _ => Some(next_sel),
    }
}

#[async_trait]
pub trait LinkLoader {
    async fn load_link(&mut self, link: &Cid) -> Result<Option<Ipld>, String>;
}

#[derive(Debug, Default)]
pub struct Progress<L = ()> {
    loader: Option<L>,
    path: Path,
    last_block: Option<LastBlockInfo>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LastBlockInfo {
    pub path: Path,
    pub link: Cid,
}

impl<L> Progress<L>
where
    L: LinkLoader + Sync + Send,
{
    pub fn new(loader: L) -> Self {
        Self {
            loader: Some(loader),
            path: Default::default(),
            last_block: None,
        }
    }
    #[async_recursion]
    pub async fn walk_adv<F>(
        &mut self,
        node: &Ipld,
        selector: Selector,
        callback: &F,
    ) -> Result<(), Error>
    where
        F: Fn(&Progress<L>, &Ipld) -> Result<(), String> + Sync,
    {
        if let Ipld::Link(cid) = node {
            if let Some(loader) = &mut self.loader {
                self.last_block = Some(LastBlockInfo {
                    path: self.path.clone(),
                    link: *cid as Cid,
                });
                let mut node = loader.load_link(cid).await.map_err(Error::Link)?;
                while let Some(Ipld::Link(c)) = node {
                    node = loader.load_link(&c).await.map_err(Error::Link)?;
                }

                if let Some(n) = node {
                    return self.walk_adv(&n, selector, callback).await;
                }
            }

            return Ok(());
        }
        callback(self, node).map_err(Error::Custom)?;

        match node {
            Ipld::StringMap(_) | Ipld::List(_) => (),
            _ => return Ok(()),
        }

        match selector.interests() {
            Some(attn) => {
                for ps in attn {
                    let v = match node.get(ps.ipld_index()) {
                        Ok(node) => node,
                        _ => continue,
                    };
                    self.visit(node, selector.clone(), callback, ps, v).await?;
                }
                Ok(())
            }
            None => {
                match node {
                    Ipld::StringMap(m) => {
                        for (k, v) in m.iter() {
                            let ps = PathSegment::from(k.as_ref());
                            self.visit(node, selector.clone(), callback, ps, v).await?;
                        }
                    }
                    Ipld::List(l) => {
                        for (i, v) in l.iter().enumerate() {
                            let ps = PathSegment::from(i);
                            self.visit(node, selector.clone(), callback, ps, v).await?;
                        }
                    }
                    _ => unreachable!(),
                }
                Ok(())
            }
        }
    }
    async fn visit<F>(
        &mut self,
        parent: &Ipld,
        selector: Selector,
        callback: &F,
        ps: PathSegment,
        node: &Ipld,
    ) -> Result<(), Error>
    where
        F: Fn(&Progress<L>, &Ipld) -> Result<(), String> + Sync,
    {
        if let Some(next_sel) = selector.explore(parent, &ps) {
            self.path.push(ps);
            self.walk_adv(node, next_sel, callback).await?;
            self.path.pop();
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Error)]
pub enum Error {
    #[error("{0}")]
    Encoding(String),
    #[error("{0}")]
    Other(&'static str),
    #[error("Failed to traverse link: {0}")]
    Link(String),
    #[error("{0}")]
    Custom(String),
}

impl From<CborError> for Error {
    fn from(err: CborError) -> Error {
        Self::Encoding(err.to_string())
    }
}

impl From<CidError> for Error {
    fn from(err: CidError) -> Error {
        Self::Custom(err.to_string())
    }
}

impl From<StdError> for Error {
    fn from(err: StdError) -> Error {
        Self::Custom(err.to_string())
    }
}

impl From<MultihashError> for Error {
    fn from(err: MultihashError) -> Error {
        Self::Custom(err.to_string())
    }
}

impl From<ProtobufError> for Error {
    fn from(err: ProtobufError) -> Error {
        Self::Encoding(err.to_string())
    }
}

pub struct StoreLoader<S = ()> {
    store: Option<S>,
}

#[async_trait]
impl<S> LinkLoader for StoreLoader<S>
where
    S: Store,
    Ipld: Decode<<<S as Store>::Params as StoreParams>::Codecs>,
{
    async fn load_link(&mut self, link: &Cid) -> Result<Option<Ipld>, String> {
        if let Some(store) = &mut self.store {
            let block = match store.get(link) {
                Ok(block) => block,
                Err(e) => return Err(e.to_string()),
            };
            let node = match block.ipld() {
                Ok(node) => node,
                Err(e) => return Err(e.to_string()),
            };
            return Ok(Some(node));
        }
        Err("MissingStore".to_string())
    }
}

pub struct BlockCallbackLoader<S, F> {
    store: Arc<S>,
    cb: F,
}

impl<S, F> BlockCallbackLoader<S, F>
where
    S: Store,
    F: FnMut(Option<Block<S::Params>>) -> Result<(), String> + Send + Sync,
{
    pub fn new(store: Arc<S>, cb: F) -> Self {
        Self { store, cb }
    }
}

#[async_trait]
impl<S, F> LinkLoader for BlockCallbackLoader<S, F>
where
    S: Store,
    F: FnMut(Option<Block<S::Params>>) -> Result<(), String> + Send + Sync,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    async fn load_link(&mut self, link: &Cid) -> Result<Option<Ipld>, String> {
        let block = match self.store.get(link) {
            Ok(block) => block,
            Err(e) => return Err(e.to_string()),
        };
        let node = match block.ipld() {
            Ok(node) => node,
            Err(e) => return Err(e.to_string()),
        };
        (self.cb)(Some(block))?;
        Ok(Some(node))
    }
}

// TODO: add a max cache size so we start evicting pending blocks to
// prevent an attack where a peer would flood us with unrelated blocks
pub struct AsyncLoader<S: Store, F> {
    store: Arc<S>,
    sender: Arc<Sender<Block<S::Params>>>,
    receiver: Arc<Receiver<Block<S::Params>>>,
    cb: F,
    /// pending blocks
    next_id: u64,
    cid: FnvHashMap<u64, Cid>,
    data: FnvHashMap<u64, Vec<u8>>,
    lookup: FnvHashMap<Cid, u64>,
}

impl<S: Store, F> AsyncLoader<S, F>
where
    F: Fn(&Block<S::Params>) -> Result<(), String> + Send + Sync,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>, cb: F) -> Self {
        let (s, r) = bounded(1000);
        Self {
            cb,
            store,
            sender: Arc::new(s),
            receiver: Arc::new(r),
            next_id: 0,
            cid: Default::default(),
            data: Default::default(),
            lookup: Default::default(),
        }
    }

    fn lookup(&mut self, cid: &Cid) -> u64 {
        if let Some(id) = self.lookup.get(cid) {
            *id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            self.lookup.insert(*cid, id);
            self.cid.insert(id, *cid);
            id
        }
    }

    fn get_pending(&mut self, cid: &Cid) -> Option<Block<S::Params>> {
        let id = self.lookup(cid);
        let cid = *self.cid.get(&id)?;
        let data = self.data.get(&id)?.clone();
        Some(Block::new_unchecked(cid, data))
    }

    fn insert_pending(&mut self, block: Block<S::Params>) {
        let id = self.lookup(block.cid());
        let (_cid, data) = block.into_inner();
        self.data.insert(id, data);
    }

    fn flush_pending(&mut self, block: &Block<S::Params>) -> Result<(), String> {
        let id = self.lookup(block.cid());
        self.cid.remove(&id);
        self.data.remove(&id);
        self.lookup.remove(block.cid());
        self.store.insert(block).map_err(|e| e.to_string())?;
        (self.cb)(block)?;
        Ok(())
    }

    pub fn sender(&self) -> Arc<Sender<Block<S::Params>>> {
        self.sender.clone()
    }
}

#[async_trait]
impl<S: Store, F> LinkLoader for AsyncLoader<S, F>
where
    F: Fn(&Block<S::Params>) -> Result<(), String> + Send + Sync,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    async fn load_link(&mut self, link: &Cid) -> Result<Option<Ipld>, String> {
        loop {
            // check if the block is already pending
            if let Some(block) = self.get_pending(link) {
                self.flush_pending(&block)?;
                match block.ipld() {
                    Ok(node) => return Ok(Some(node)),
                    Err(e) => return Err(e.to_string()),
                }
            }
            // check if it is already in the blockstore
            if let Ok(block) = self.store.get(link) {
                match block.ipld() {
                    Ok(node) => return Ok(Some(node)),
                    Err(e) => return Err(e.to_string()),
                };
            }
            match self.receiver.recv().await {
                Ok(block) => {
                    if block.cid() == link {
                        self.store.insert(&block).map_err(|e| e.to_string())?;
                        (self.cb)(&block)?;
                        match block.ipld() {
                            Ok(node) => return Ok(Some(node)),
                            Err(e) => return Err(e.to_string()),
                        }
                    }
                    self.insert_pending(block);
                }
                Err(e) => return Err(e.to_string()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex;
    use libipld::block::Block;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::mem::MemStore;
    use libipld::multihash::Code;
    use libipld::DefaultParams;
    use std::sync::{Arc, Mutex};

    struct ExpectVisit {
        path: Path,
    }

    struct TestData {
        node: Ipld,
        loader: StoreLoader<MemStore<DefaultParams>>,
        expect_full_traversal: [ExpectVisit; 12],
        expect_entries_traversal: [ExpectVisit; 4],
    }

    fn gen_data() -> TestData {
        let store = MemStore::<DefaultParams>::default();

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();
        store.insert(&leaf1_block);

        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();
        store.insert(&leaf2_block);

        let parent = ipld!({
            "children": [leaf1_block.cid(), leaf2_block.cid()],
            "favouriteChild": leaf2_block.cid(),
            "name": "parent",
        });
        let parent_block = Block::encode(DagCborCodec, Code::Sha2_256, &parent).unwrap();
        store.insert(&parent_block);

        let expect_full_traversal: [ExpectVisit; 12] = [
            ExpectVisit {
                path: Path::from(""),
            },
            ExpectVisit {
                path: Path::from("children"),
            },
            ExpectVisit {
                path: Path::from("children/0"),
            },
            ExpectVisit {
                path: Path::from("children/0/name"),
            },
            ExpectVisit {
                path: Path::from("children/0/size"),
            },
            ExpectVisit {
                path: Path::from("children/1"),
            },
            ExpectVisit {
                path: Path::from("children/1/name"),
            },
            ExpectVisit {
                path: Path::from("children/1/size"),
            },
            ExpectVisit {
                path: Path::from("favouriteChild"),
            },
            ExpectVisit {
                path: Path::from("favouriteChild/name"),
            },
            ExpectVisit {
                path: Path::from("favouriteChild/size"),
            },
            ExpectVisit {
                path: Path::from("name"),
            },
        ];
        let expect_entries_traversal: [ExpectVisit; 4] = [
            ExpectVisit {
                path: Path::from(""),
            },
            ExpectVisit {
                path: Path::from("children"),
            },
            ExpectVisit {
                path: Path::from("favouriteChild"),
            },
            ExpectVisit {
                path: Path::from("name"),
            },
        ];

        let loader = StoreLoader { store: Some(store) };

        TestData {
            node: parent,
            loader,
            expect_full_traversal,
            expect_entries_traversal,
        }
    }

    #[async_std::test]
    async fn full_traversal() {
        let TestData {
            node,
            loader,
            expect_full_traversal: expect,
            ..
        } = gen_data();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let mut progress = Progress {
            loader: Some(loader),
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&node, selector, &|prog, ipld| -> Result<(), String> {
                let mut idx = index.lock().unwrap();
                let exp = &expect[*idx];
                assert_eq!(prog.path, exp.path);
                *idx += 1;
                Ok(())
            })
            .await
            .unwrap();

        let current_idx = *index.lock().unwrap();
        assert_eq!(current_idx, 12)
    }

    #[async_std::test]
    async fn full_traversal_json_sel() {
        let TestData {
            node,
            loader,
            expect_full_traversal: expect,
            ..
        } = gen_data();

        let sel_data = r#"
            {
                "R": {
                    "l": {
                        "none": {}
                    },
                    ":>": {
                        "a": {
                            ">": {
                                "@": {}
                            }
                        }
                    }
                }
            }"#;
        let selector: Selector = serde_json::from_str(sel_data).unwrap();

        let mut progress = Progress {
            loader: Some(loader),
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&node, selector, &|prog, ipld| -> Result<(), String> {
                let mut idx = index.lock().unwrap();
                let exp = &expect[*idx];
                assert_eq!(prog.path, exp.path);
                *idx += 1;
                Ok(())
            })
            .await
            .unwrap();

        let current_idx = *index.lock().unwrap();
        assert_eq!(current_idx, 12)
    }

    #[async_std::test]
    async fn full_traversal_cbor_sel() {
        let TestData {
            node,
            loader,
            expect_full_traversal: expect,
            ..
        } = gen_data();

        let sel_data = hex::decode("a16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0").unwrap();
        let selector = Selector::unmarshal_cbor(&sel_data).unwrap();

        let mut progress = Progress {
            loader: Some(loader),
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&node, selector, &|prog, ipld| -> Result<(), String> {
                let mut idx = index.lock().unwrap();
                let exp = &expect[*idx];
                assert_eq!(prog.path, exp.path);
                *idx += 1;
                Ok(())
            })
            .await
            .unwrap();

        let current_idx = *index.lock().unwrap();
        assert_eq!(current_idx, 12)
    }

    #[async_std::test]
    async fn entries_traversal() {
        let TestData {
            node,
            loader,
            expect_entries_traversal: expect,
            ..
        } = gen_data();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::Depth(2),
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let mut progress = Progress {
            loader: Some(loader),
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&node, selector, &|prog, ipld| -> Result<(), String> {
                let mut idx = index.lock().unwrap();
                let exp = &expect[*idx];
                assert_eq!(prog.path, exp.path);
                *idx += 1;
                Ok(())
            })
            .await
            .unwrap();

        let current_idx = *index.lock().unwrap();
        assert_eq!(current_idx, 4)
    }

    #[async_std::test]
    async fn async_loader() {
        use futures::join;

        let store = Arc::new(MemStore::<DefaultParams>::default());

        let unrelated1 = ipld!({ "name": "not in this tree" });
        let unrelated1_block = Block::encode(DagCborCodec, Code::Sha2_256, &unrelated1).unwrap();
        let unrelated2 = ipld!({ "name": "garbage" });
        let unrelated2_block = Block::encode(DagCborCodec, Code::Sha2_256, &unrelated2).unwrap();

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();

        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();

        let parent = ipld!({
            "children": [leaf1_block.cid(), leaf2_block.cid()],
            "favouriteChild": leaf2_block.cid(),
            "name": "parent",
        });
        let parent_block = Block::encode(DagCborCodec, Code::Sha2_256, &parent).unwrap();

        let loader = AsyncLoader::new(store.clone(), |_| Ok(()));

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let index = Arc::new(Mutex::new(0));

        let idxmut = index.clone();

        let sender = loader.sender();
        join!(
            async move {
                let mut progress = Progress {
                    loader: Some(loader),
                    path: Path::default(),
                    last_block: None,
                };

                progress
                    .walk_adv(&parent, selector, &|prog, ipld| -> Result<(), String> {
                        let mut idx = idxmut.lock().unwrap();
                        *idx += 1;
                        Ok(())
                    })
                    .await
                    .unwrap();
            },
            async move {
                sender.send(unrelated1_block).await.unwrap();
                sender.send(leaf1_block).await.unwrap();
                sender.send(leaf2_block).await.unwrap();
                sender.send(unrelated2_block).await.unwrap();
                sender.send(parent_block).await.unwrap();
            },
        );

        let current_idx = *index.lock().unwrap();
        assert_eq!(current_idx, 12)
    }
}
