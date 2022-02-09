use super::traversal::{BlockCallbackLoader, Progress, Selector};
use super::{
    Extensions, GraphsyncHooks, GraphsyncMessage, GraphsyncRequest, GraphsyncResponse, Prefix,
    RequestId, ResponseStatusCode,
};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::task::{Context, Poll};
use blockstore::types::BlockStore;
use futures_lite::stream::StreamExt;
use libipld::codec::Decode;
use libipld::ipld::Ipld;
use libipld::store::StoreParams;
use libipld::{Block, Cid};
use libp2p::core::PeerId;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

#[cfg(not(target_os = "unknown"))]
use async_std::task::spawn;

#[cfg(target_os = "unknown")]
use async_std::task::spawn_local as spawn;

const MAX_BLOCK_SIZE: usize = 512 * 1024;

#[derive(Debug)]
pub enum ResponseEvent {
    Partial(PeerId, GraphsyncMessage),
    Completed(PeerId, GraphsyncMessage),
}

#[derive(Debug)]
pub struct ResponseBuilder<P: StoreParams> {
    req_id: RequestId,
    peer_id: PeerId,
    size: usize,
    blocks: VecDeque<Block<P>>,
    sender: Arc<Sender<ResponseEvent>>,
    sent: HashSet<Cid>,
    extensions: Extensions,
}

impl<P: StoreParams> ResponseBuilder<P> {
    pub fn new(req_id: RequestId, peer_id: PeerId, sender: Arc<Sender<ResponseEvent>>) -> Self {
        Self {
            req_id,
            peer_id,
            size: 0,
            blocks: VecDeque::new(),
            sender,
            sent: HashSet::new(),
            extensions: Default::default(),
        }
    }
    pub fn add_block(&mut self, block: Block<P>) {
        let next_size = self.size + block.data().len();
        // batch all blocks together until we get to the max size
        if next_size > MAX_BLOCK_SIZE {
            // flush the blocks to the network
            self.send(ResponseStatusCode::RequestCompletedPartial);
        }
        self.blocks.push_back(block);
        self.size = next_size;
    }
    pub fn completed(&mut self) {
        self.send(ResponseStatusCode::RequestCompletedFull);
    }
    pub fn failed(&mut self, _err: String) {
        self.send(ResponseStatusCode::RequestFailedUnknown);
    }
    pub fn not_found(&mut self) {
        self.send(ResponseStatusCode::RequestFailedContentNotFound);
    }
    pub fn rejected(&mut self) {
        self.send(ResponseStatusCode::RequestRejected);
    }
    fn send(&mut self, status: ResponseStatusCode) {
        let mut msg = GraphsyncMessage::default();
        msg.responses.insert(
            self.req_id,
            GraphsyncResponse {
                id: self.req_id,
                status,
                extensions: self.extensions.clone(),
            },
        );
        while let Some(blk) = self.blocks.pop_front() {
            let (cid, data) = blk.clone().into_inner();
            if !self.sent.contains(&cid) {
                msg.blocks.push((Prefix::from(cid).to_bytes(), data));
                self.sent.insert(cid);
            }
        }
        let event = match status {
            ResponseStatusCode::RequestCompletedFull => ResponseEvent::Completed(self.peer_id, msg),
            _ => ResponseEvent::Partial(self.peer_id, msg),
        };
        match self.sender.try_send(event) {
            Ok(()) => {}
            Err(_) => {}
        }
    }
    fn set_extensions(&mut self, extensions: Extensions) {
        self.extensions = extensions;
    }
}

pub struct ResponseManager<S> {
    store: Arc<S>,
    sender: Arc<Sender<ResponseEvent>>,
    receiver: Arc<Mutex<Receiver<ResponseEvent>>>,
    hooks: Arc<RwLock<GraphsyncHooks>>,
}

impl<S> ResponseManager<S>
where
    S: BlockStore + 'static,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>, hooks: Arc<RwLock<GraphsyncHooks>>) -> Self {
        let (s, r) = bounded(1000);
        Self {
            store,
            sender: Arc::new(s),
            receiver: Arc::new(Mutex::new(r)),
            hooks,
        }
    }
    pub fn inject_request(&self, peer: PeerId, request: &GraphsyncRequest) {
        let Self {
            store,
            sender,
            hooks,
            ..
        } = self;
        let store = Arc::clone(store);
        let sender = Arc::clone(sender);
        let id = request.id;
        let mut builder = ResponseBuilder::new(id, peer, sender);
        // validate request and add any extensions
        let (approved, extensions) = (hooks.read().unwrap().incoming_request)(peer, request);
        builder.set_extensions(extensions);
        if !approved {
            builder.rejected();
            return;
        }
        let root = request.root;
        let selector = request.selector.clone();
        let hooks = hooks.clone();
        spawn(async move {
            if let Ok(block) = store.get(&root) {
                builder.set_extensions((hooks.read().unwrap().outgoing_block)(
                    peer,
                    root,
                    block.data().len(),
                ));
                builder.add_block(block.clone());
                if let Ok(node) = block.ipld() {
                    let loader = BlockCallbackLoader::new(store, |block| {
                        match block {
                            Some(block) => {
                                builder.set_extensions((hooks.read().unwrap().outgoing_block)(
                                    peer,
                                    block.cid().clone(),
                                    block.data().len(),
                                ));
                                builder.add_block(block);
                            }
                            None => {}
                        };
                        Ok(())
                    });

                    let mut progress = Progress::new(loader);
                    match progress.walk_adv(&node, selector, &|_, _| Ok(())).await {
                        Ok(()) => {
                            builder.completed();
                        }
                        Err(e) => {
                            builder.failed(e.to_string());
                        }
                    }
                } else {
                    builder.failed("Could not decode node".to_string());
                }
            } else {
                builder.not_found();
            }
        });
    }

    pub fn next(&self, ctx: &mut Context) -> Poll<Option<ResponseEvent>> {
        self.receiver.lock().unwrap().poll_next(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traversal::RecursionLimit;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;

    struct TestData {
        root: Cid,
        store: Arc<MemoryBlockStore>,
    }

    fn gen_data() -> TestData {
        let store = Arc::new(MemoryBlockStore::default());

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

        TestData {
            root: parent_block.cid().clone(),
            store,
        }
    }

    #[async_std::test]
    async fn test_responses() {
        let TestData { root, store } = gen_data();

        let hooks = Arc::new(RwLock::new(GraphsyncHooks::default()));

        let manager = ResponseManager::new(store.clone(), hooks);

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        manager.inject_request(
            PeerId::random(),
            &GraphsyncRequest {
                root,
                selector,
                id: 1,
                extensions: Default::default(),
            },
        );
        if let Ok(ResponseEvent::Completed(_peer, msg)) =
            manager.receiver.lock().unwrap().recv().await
        {
            assert_eq!(msg.responses.len(), 1);
            assert_eq!(msg.blocks.len(), 3);
            assert_eq!(
                msg.responses.get(&1).unwrap().status,
                ResponseStatusCode::RequestCompletedFull
            );
        };
    }
}
