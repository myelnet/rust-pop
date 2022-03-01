use crate::empty_map;
use async_recursion::async_recursion;
use async_std::channel::{bounded, Receiver, Sender};
use async_trait::async_trait;
use blockstore::{errors::Error as BsError, types::BlockStore};
use fnv::FnvHashMap;
use libipld::cid::Error as CidError;
use libipld::codec::Decode;
use libipld::ipld::{Ipld, IpldIndex};
use libipld::multihash::Error as MultihashError;
use libipld::store::StoreParams;
use libipld::{Block, Cid};
use protobuf::ProtobufError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_cbor::{error::Error as CborError, from_slice, to_vec};
use smallvec::SmallVec;
use std::collections::HashSet;
use std::fmt;
use std::io::Error as StdError;
use std::sync::Arc;
use std::vec;
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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
            PathSegment::Int(int) => IpldIndex::List(*int),
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

#[derive(Debug, PartialEq, Eq, Hash, Default, Clone)]
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
                        Some(ExploreRecursive {
                            current: replace_recursive_edge(next, Some(*sequence.clone()))
                                .map(Box::new),
                            sequence,
                            limit: RecursionLimit::Depth(depth - 1),
                        })
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
    matches!(next_sel, ExploreRecursiveEdge { .. })
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

/// Executes an iterative traversal based on the give root CID and selector
/// and returns the blocks resolved along the way.
#[derive(Debug)]
pub struct BlockIterator<S> {
    store: Arc<S>,
    selector: Selector,
    start: Option<Cid>,
    stack_list: SmallVec<[vec::IntoIter<(Ipld, Selector)>; 8]>,
    seen: Option<HashSet<Cid>>,
    restart: bool,
}

impl<S> Iterator for BlockIterator<S>
where
    S: BlockStore,
    Ipld: Decode<<<S as BlockStore>::Params as StoreParams>::Codecs>,
{
    type Item = Result<Block<S::Params>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cid) = self.start.take() {
            if let Some(block) = self.handle_node(Ipld::Link(cid), self.selector.clone()) {
                return Some(block);
            }
        }
        while !self.stack_list.is_empty() {
            let next = self
                .stack_list
                .last_mut()
                .expect("stack should be non-empty")
                .next();
            match next {
                None => {
                    self.stack_list.pop();
                }
                Some((node, selector)) => {
                    if let Some(block) = self.handle_node(node, selector) {
                        return Some(block);
                    }
                }
            }
        }
        None
    }
}

impl<S> BlockIterator<S>
where
    S: BlockStore,
    Ipld: Decode<<<S as BlockStore>::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>, root: Cid, selector: Selector) -> Self {
        Self {
            store,
            selector,
            start: Some(root),
            stack_list: SmallVec::new(),
            seen: None,
            restart: false,
        }
    }
    /// if activated, the traversal will not revisit the same block if it is linked
    /// somewhere else.
    pub fn ignore_duplicate_links(mut self) -> Self {
        self.seen = Some(HashSet::default());
        self
    }
    /// if activated, the traversal will always reattempt to traverse from the
    /// last missing link. May cause an infinite loop if the the block is never inserted
    /// into the underlying blockstore. To continue traversal even if a block is missing,
    /// the underlying blockstore should return a SkipMe or custom error.
    pub fn restart_missing_link(mut self) -> Self {
        self.restart = true;
        self
    }

    fn handle_node(
        &mut self,
        mut ipld: Ipld,
        selector: Selector,
    ) -> Option<Result<Block<S::Params>, Error>> {
        let maybe_block = self.maybe_resolve_link(&mut ipld);
        match ipld {
            Ipld::StringMap(_) | Ipld::List(_) => self.push(ipld, selector.clone()),
            _ => {}
        }
        match maybe_block {
            Some(Err(Error::BlockNotFound(cid))) => {
                // if the block is missing the next iteration will restart from there
                if self.restart {
                    self.start = Some(cid);
                    self.selector = selector;
                }
                Some(Err(Error::BlockNotFound(cid)))
            }
            mb => mb,
        }
    }

    fn maybe_resolve_link(&mut self, ipld: &mut Ipld) -> Option<Result<Block<S::Params>, Error>> {
        let mut result = None;
        while let Ipld::Link(cid) = ipld {
            if let Some(ref mut seen) = self.seen {
                if !seen.insert(*cid) {
                    break;
                }
            }
            let block = match self.store.get(cid) {
                Ok(block) => block,
                Err(e) => return Some(Err(Error::from(e))),
            };
            *ipld = match block.ipld() {
                Ok(node) => node,
                Err(e) => return Some(Err(Error::Encoding(e.to_string()))),
            };
            result = Some(Ok(block));
        }
        result
    }
    fn push(&mut self, ipld: Ipld, selector: Selector) {
        self.stack_list
            .push(select_next_entries(ipld, selector).into_iter());
    }
}

/// returns a list of ipld values selected by the given selector.
/// TODO: there should be a way to return an iterator so it can be lazily evaluated.
fn select_next_entries(ipld: Ipld, selector: Selector) -> Vec<(Ipld, Selector)> {
    match selector.interests() {
        Some(attn) => attn
            .into_iter()
            .filter_map(|ps| {
                if let Some(next_sel) = selector.clone().explore(&ipld, &ps) {
                    let v = match ipld.get(ps.ipld_index()) {
                        Ok(node) => node,
                        _ => return None,
                    };
                    return Some((v.clone(), next_sel));
                }
                None
            })
            .collect(),
        None => match &ipld {
            Ipld::StringMap(m) => m
                .keys()
                .filter_map(|k| {
                    let ps = PathSegment::from(k.as_ref());
                    if let Some(next_sel) = selector.clone().explore(&ipld, &ps) {
                        let v = match ipld.get(ps.ipld_index()) {
                            Ok(node) => node,
                            _ => return None,
                        };
                        return Some((v.clone(), next_sel));
                    }
                    None
                })
                .collect(),
            Ipld::List(l) => l
                .iter()
                .enumerate()
                .filter_map(|(i, _)| {
                    let ps = PathSegment::from(i);
                    if let Some(next_sel) = selector.clone().explore(&ipld, &ps) {
                        let v = match ipld.get(ps.ipld_index()) {
                            Ok(node) => node,
                            _ => return None,
                        };
                        return Some((v.clone(), next_sel));
                    }
                    None
                })
                .collect(),
            _ => Vec::new(),
        },
    }
}

#[derive(Debug, Default)]
pub struct Progress<L> {
    pub loader: L,
    pub path: Path,
    pub last_block: Option<LastBlockInfo>,
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
            loader,
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
            self.last_block = Some(LastBlockInfo {
                path: self.path.clone(),
                link: *cid as Cid,
            });
            let mut node = self.loader.load_link(cid).await.map_err(Error::Link)?;
            while let Some(Ipld::Link(c)) = node {
                node = self.loader.load_link(&c).await.map_err(Error::Link)?;
            }

            if let Some(n) = node {
                return self.walk_adv(&n, selector, callback).await;
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
    #[error("BlockStore: block not found for {0}")]
    BlockNotFound(Cid),
    #[error("BlockStore: skipping block for {0}")]
    SkipMe(Cid),
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

impl From<BsError> for Error {
    fn from(err: BsError) -> Error {
        match err {
            BsError::BlockNotFound(c) => Self::BlockNotFound(c),
            BsError::SkipMe(c) => Self::SkipMe(c),
            e => Self::Custom(e.to_string()),
        }
    }
}

pub struct StoreLoader<S> {
    pub store: S,
}

#[async_trait]
impl<S> LinkLoader for StoreLoader<S>
where
    S: BlockStore,
    Ipld: Decode<<<S as BlockStore>::Params as StoreParams>::Codecs>,
{
    async fn load_link(&mut self, link: &Cid) -> Result<Option<Ipld>, String> {
        let block = match self.store.get(link) {
            Ok(block) => block,
            Err(_) => return Ok(None),
        };
        let node = match block.ipld() {
            Ok(node) => node,
            Err(e) => return Err(e.to_string()),
        };
        Ok(Some(node))
    }
}

pub struct BlockCallbackLoader<S, F> {
    store: Arc<S>,
    cb: F,
}

impl<S, F> BlockCallbackLoader<S, F>
where
    S: BlockStore,
    F: FnMut(&Cid, Option<Block<S::Params>>) -> Result<(), String> + Send + Sync,
{
    pub fn new(store: Arc<S>, cb: F) -> Self {
        Self { store, cb }
    }
}

#[async_trait]
impl<S, F> LinkLoader for BlockCallbackLoader<S, F>
where
    S: BlockStore,
    F: FnMut(&Cid, Option<Block<S::Params>>) -> Result<(), String> + Send + Sync,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    async fn load_link(&mut self, link: &Cid) -> Result<Option<Ipld>, String> {
        let block = match self.store.get(link) {
            Ok(block) => block,
            Err(_) => {
                (self.cb)(link, None)?;
                return Ok(None);
            }
        };
        let node = match block.ipld() {
            Ok(node) => node,
            Err(e) => return Err(e.to_string()),
        };
        (self.cb)(link, Some(block))?;
        Ok(Some(node))
    }
}

#[derive(Debug, Clone)]
pub struct BlockData {
    pub link: Cid,
    pub data: Ipld,
    pub size: usize,
}

// TODO: add a max cache size so we start evicting pending blocks to
// prevent an attack where a peer would flood us with unrelated blocks
pub struct AsyncLoader<S: BlockStore, F> {
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

impl<S: BlockStore, F> AsyncLoader<S, F>
where
    F: Fn(BlockData) -> Result<(), String> + Send + Sync,
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
        // (self.cb)(block)?;
        Ok(())
    }

    pub fn sender(&self) -> Arc<Sender<Block<S::Params>>> {
        self.sender.clone()
    }
}

#[async_trait]
impl<S: BlockStore, F> LinkLoader for AsyncLoader<S, F>
where
    F: Fn(BlockData) -> Result<(), String> + Send + Sync,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    async fn load_link(&mut self, link: &Cid) -> Result<Option<Ipld>, String> {
        loop {
            // check if the block is already pending
            if let Some(block) = self.get_pending(link) {
                self.flush_pending(&block)?;
                match block.ipld() {
                    Ok(node) => {
                        (self.cb)(BlockData {
                            link: *link,
                            size: block.data().len(),
                            data: node.clone(),
                        })?;
                        return Ok(Some(node));
                    }
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
                        match block.ipld() {
                            Ok(node) => {
                                (self.cb)(BlockData {
                                    link: *link,
                                    size: block.data().len(),
                                    data: node.clone(),
                                })?;
                                return Ok(Some(node));
                            }
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
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use hex;
    use libipld::block::Block;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use std::sync::{Arc, Mutex};

    struct ExpectVisit {
        path: Path,
    }

    struct TestData {
        node: Ipld,
        loader: StoreLoader<MemoryBlockStore>,
        expect_full_traversal: [ExpectVisit; 12],
        expect_entries_traversal: [ExpectVisit; 4],
    }

    fn gen_data() -> TestData {
        let store = MemoryBlockStore::default();

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();
        store.insert(&leaf1_block).unwrap();

        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();
        store.insert(&leaf2_block).unwrap();

        let parent = ipld!({
            "children": [leaf1_block.cid(), leaf2_block.cid()],
            "favouriteChild": leaf2_block.cid(),
            "name": "parent",
        });
        let parent_block = Block::encode(DagCborCodec, Code::Sha2_256, &parent).unwrap();
        store.insert(&parent_block).unwrap();

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

        let loader = StoreLoader { store };

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
            loader,
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&node, selector, &|prog, _ipld| -> Result<(), String> {
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
            loader,
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&node, selector, &|prog, _ipld| -> Result<(), String> {
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
            loader,
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&node, selector, &|prog, _ipld| -> Result<(), String> {
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
            loader,
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&node, selector, &|prog, _ipld| -> Result<(), String> {
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

        let store = Arc::new(MemoryBlockStore::default());

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
                    loader,
                    path: Path::default(),
                    last_block: None,
                };

                progress
                    .walk_adv(&parent, selector, &|_prog, _ipld| -> Result<(), String> {
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

    #[async_std::test]
    async fn traverse_missing_blocks() {
        let store = MemoryBlockStore::default();

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();
        store.insert(&leaf1_block).unwrap();

        // leaf2 is not present in the blockstore
        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block =
            Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();

        let parent = ipld!({
            "children": [leaf1_block.cid(), leaf2_block.cid()],
            "favouriteChild": leaf2_block.cid(),
            "name": "parent",
        });
        let parent_block = Block::encode(DagCborCodec, Code::Sha2_256, &parent).unwrap();
        store.insert(&parent_block).unwrap();

        let expect: [ExpectVisit; 6] = [
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
                path: Path::from("name"),
            },
        ];
        let loader = StoreLoader { store };

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let mut progress = Progress {
            loader,
            path: Path::default(),
            last_block: None,
        };

        let index = Arc::new(Mutex::new(0));
        progress
            .walk_adv(&parent, selector, &|prog, _ipld| -> Result<(), String> {
                let mut idx = index.lock().unwrap();
                let exp = &expect[*idx];
                assert_eq!(prog.path, exp.path);
                *idx += 1;
                Ok(())
            })
            .await
            .unwrap();

        let current_idx = *index.lock().unwrap();
        assert_eq!(current_idx, 6)
    }

    #[test]
    fn test_walk_next() {
        let store = Arc::new(MemoryBlockStore::default());

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();
        store.insert(&leaf1_block).unwrap();

        // leaf2 is not present in the blockstore
        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block =
            Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();
        store.insert(&leaf2_block).unwrap();

        let parent = ipld!({
            "children": [leaf1_block.cid(), leaf2_block.cid()],
            "favouriteChild": leaf2_block.cid(),
            "name": "parent",
        });
        let parent_block = Block::encode(DagCborCodec, Code::Sha2_256, &parent).unwrap();
        store.insert(&parent_block).unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };
        let mut it = BlockIterator::new(store, *parent_block.cid(), selector);

        let first = it.next().unwrap().unwrap();
        assert_eq!(first, parent_block);

        let second = it.next().unwrap().unwrap();
        assert_eq!(second, leaf1_block);

        let third = it.next().unwrap().unwrap();
        assert_eq!(third, leaf2_block);

        let last = it.next().unwrap().unwrap();
        assert_eq!(last, leaf2_block);

        let end = it.next();
        assert_eq!(end, None);
    }

    #[test]
    fn test_walk_next_missing() {
        let store = Arc::new(MemoryBlockStore::default());

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();

        // leaf2 is not present in the blockstore
        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block =
            Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();

        let parent = ipld!({
            "children": [leaf1_block.cid(), leaf2_block.cid()],
            "favouriteChild": leaf2_block.cid(),
            "name": "parent",
        });
        let parent_block = Block::encode(DagCborCodec, Code::Sha2_256, &parent).unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };
        let mut it =
            BlockIterator::new(store.clone(), *parent_block.cid(), selector).restart_missing_link();

        let first = it.next().unwrap();
        assert_eq!(first, Err(Error::BlockNotFound(*parent_block.cid())));

        let first = it.next().unwrap();
        assert_eq!(first, Err(Error::BlockNotFound(*parent_block.cid())));

        store.insert(&parent_block).unwrap();
        let first = it.next().unwrap().unwrap();
        assert_eq!(first, parent_block);

        let second = it.next().unwrap();
        assert_eq!(second, Err(Error::BlockNotFound(*leaf1_block.cid())));

        store.insert(&leaf1_block).unwrap();
        let second = it.next().unwrap().unwrap();
        assert_eq!(second, leaf1_block);

        let third = it.next().unwrap();
        assert_eq!(third, Err(Error::BlockNotFound(*leaf2_block.cid())));

        store.insert(&leaf2_block).unwrap();
        let third = it.next().unwrap().unwrap();
        assert_eq!(third, leaf2_block);

        let last = it.next().unwrap().unwrap();
        assert_eq!(last, leaf2_block);

        let end = it.next();
        assert_eq!(end, None);
    }
}
