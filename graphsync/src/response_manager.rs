use super::traversal::{BlockCallbackLoader, Progress, Selector};
use super::{
    Extensions, GraphsyncHooks, GraphsyncMessage, GraphsyncRequest, GraphsyncResponse, Prefix,
    RequestId, ResponseStatusCode,
};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::task::{Context, Poll};
use blockstore::types::BlockStore;
use filecoin::cid_helpers::CidCbor;
use futures_lite::stream::StreamExt;
use libipld::codec::Decode;
use libipld::ipld::Ipld;
use libipld::store::StoreParams;
use libipld::{Block, Cid};
use libp2p::core::PeerId;
use serde::{Deserialize, Serialize};
use serde_cbor::to_vec;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

#[cfg(not(target_os = "unknown"))]
use async_std::task::spawn;

#[cfg(target_os = "unknown")]
use async_std::task::spawn_local as spawn;

const MAX_BLOCK_SIZE: usize = 512 * 1024;
pub static METADATA_EXTENSION: &str = "graphsync/response-metadata";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataItem {
    link: CidCbor,
    block_present: bool,
}

#[derive(Debug)]
pub enum ResponseEvent {
    Accepted(PeerId, GraphsyncRequest),
    Partial(PeerId, GraphsyncMessage),
    Completed(PeerId, GraphsyncMessage),
}

#[derive(Debug)]
pub struct ResponseBuilder<P: StoreParams> {
    req_id: RequestId,
    peer_id: PeerId,
    size: usize,
    partial: bool,
    blocks: VecDeque<Block<P>>,
    sender: Arc<Sender<ResponseEvent>>,
    sent: HashSet<Cid>,
    extensions: Extensions,
    metadata: Vec<MetadataItem>,
}

impl<P: StoreParams> ResponseBuilder<P> {
    pub fn new(req_id: RequestId, peer_id: PeerId, sender: Arc<Sender<ResponseEvent>>) -> Self {
        Self {
            req_id,
            peer_id,
            size: 0,
            partial: false,
            blocks: VecDeque::new(),
            sender,
            sent: HashSet::new(),
            extensions: Default::default(),
            metadata: Vec::new(),
        }
    }
    pub fn add_block(&mut self, block: Block<P>) {
        let block_size = block.data().len();
        let next_size = self.size + block_size;
        // batch all blocks together until we get to the max size
        if next_size > MAX_BLOCK_SIZE {
            // flush the blocks to the network
            self.send(ResponseStatusCode::PartialResponse);
        }
        self.metadata.push(MetadataItem {
            link: CidCbor::from(block.cid().clone()),
            block_present: true,
        });
        self.blocks.push_back(block);
        self.size = self.size + block_size;
    }
    pub fn missing_block(&mut self, cid: &Cid) {
        self.metadata.push(MetadataItem {
            link: CidCbor::from(cid.clone()),
            block_present: false,
        });
        self.partial = true;
    }
    pub fn completed(&mut self) {
        let event = {
            if self.partial {
                ResponseStatusCode::RequestCompletedPartial
            } else {
                ResponseStatusCode::RequestCompletedFull
            }
        };
        self.send(event);
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
        let metadata = to_vec(&self.metadata).expect("Expected metadata to encode");
        let mut extensions = self.extensions.clone();
        extensions.insert(METADATA_EXTENSION.to_string(), metadata);
        msg.responses.insert(
            self.req_id,
            GraphsyncResponse {
                id: self.req_id,
                status,
                extensions,
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
            ResponseStatusCode::RequestCompletedPartial => {
                ResponseEvent::Completed(self.peer_id, msg)
            }
            _ => ResponseEvent::Partial(self.peer_id, msg),
        };
        match self.sender.try_send(event) {
            Ok(()) => {
                //reset extensions after message is sent
                self.extensions = Default::default();
                self.size = 0;
            }
            Err(_) => {}
        }
    }
    fn add_extensions(&mut self, extensions: Extensions) {
        self.extensions.extend(extensions);
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
        let mut builder = ResponseBuilder::new(id, peer, sender.clone());
        // validate request and add any extensions
        let (approved, extensions) = (hooks.read().unwrap().incoming_request)(&peer, request);
        builder.add_extensions(extensions);
        if !approved {
            builder.rejected();
            return;
        }
        sender
            .try_send(ResponseEvent::Accepted(peer, request.clone()))
            .unwrap();
        let root = request.root;
        let selector = request.selector.clone();
        let hooks = hooks.clone();
        spawn(async move {
            if let Ok(block) = store.get(&root) {
                builder.add_extensions((hooks.read().unwrap().outgoing_block)(
                    &peer,
                    &root,
                    block.data().len(),
                ));
                builder.add_block(block.clone());
                if let Ok(node) = block.ipld() {
                    let loader = BlockCallbackLoader::new(store, |link, block| {
                        match block {
                            Some(block) => {
                                builder.add_extensions((hooks.read().unwrap().outgoing_block)(
                                    &peer,
                                    block.cid(),
                                    block.data().len(),
                                ));
                                builder.add_block(block);
                            }
                            None => {
                                builder.missing_block(link);
                            }
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
    use libipld::store::DefaultParams;

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
        if let Ok(ResponseEvent::Accepted(_peer, req)) =
            manager.receiver.lock().unwrap().recv().await
        {
            assert_eq!(req.id, 1);
        } else {
            panic!("received unexpected event");
        }
        if let Ok(ResponseEvent::Completed(_peer, msg)) =
            manager.receiver.lock().unwrap().recv().await
        {
            assert_eq!(msg.responses.len(), 1);
            assert_eq!(msg.blocks.len(), 3);
            assert_eq!(
                msg.responses.get(&1).unwrap().status,
                ResponseStatusCode::RequestCompletedFull
            );
        } else {
            panic!("received unexpected event");
        };
    }

    #[async_std::test]
    async fn response_missing_block() {
        use serde_cbor::from_slice;

        let store = Arc::new(MemoryBlockStore::default());

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();
        store.insert(&leaf1_block).unwrap();

        // leaf2 is missing in the blockstore
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

        let hooks = Arc::new(RwLock::new(GraphsyncHooks::default()));

        let manager = ResponseManager::new(store, hooks);

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
                root: parent_block.cid().clone(),
                selector,
                id: 1,
                extensions: Default::default(),
            },
        );
        if let Ok(ResponseEvent::Accepted(_peer, req)) =
            manager.receiver.lock().unwrap().recv().await
        {
            assert_eq!(req.id, 1);
        } else {
            panic!("received unexpected event");
        }
        if let Ok(ResponseEvent::Completed(_peer, msg)) =
            manager.receiver.lock().unwrap().recv().await
        {
            assert_eq!(msg.responses.len(), 1);
            assert_eq!(msg.blocks.len(), 2);
            assert_eq!(
                msg.responses.get(&1).unwrap().status,
                ResponseStatusCode::RequestCompletedPartial
            );

            let metabytes = msg.responses[&1]
                .extensions
                .get(METADATA_EXTENSION)
                .unwrap();
            let metadata: Vec<MetadataItem> = from_slice(metabytes).unwrap();
            metadata
                .iter()
                .find(|item| item.block_present == false)
                .unwrap();
        } else {
            panic!("received unexpected event");
        };
    }
}
