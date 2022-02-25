use super::traversal::{BlockIterator, Error, Selector};
use super::{Extensions, GraphsyncMessage, GraphsyncRequest, Prefix, RequestId};
use blockstore::types::BlockStore;
use fnv::FnvHashMap;
use libipld::{codec::Decode, store::StoreParams, Block, Cid, Ipld};
use libp2p::core::PeerId;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, Mutex,
};

#[derive(Debug)]
pub struct RequestManager<S: BlockStore> {
    id_counter: Arc<AtomicI32>,
    store: Arc<S>,
    ongoing: Arc<Mutex<FnvHashMap<RequestId, BlockIterator<S>>>>,
}

impl<S: BlockStore + 'static> RequestManager<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            ongoing: Default::default(),
            id_counter: Arc::new(AtomicI32::new(1)),
        }
    }
}
