use crate::graphsync::{
    GraphsyncMessage, GraphsyncResponse, Prefix, RequestId, ResponseStatusCode,
};
use crate::traversal::{BlockCallbackLoader, Progress, Selector};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::task::{Context, Poll};
use futures_lite::stream::StreamExt;
use libipld::codec::Decode;
use libipld::ipld::Ipld;
use libipld::store::{Store, StoreParams};
use libipld::{Block, Cid};
use libp2p::core::PeerId;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

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
    fn send(&mut self, status: ResponseStatusCode) {
        let mut msg = GraphsyncMessage::default();
        msg.responses.insert(
            self.req_id,
            GraphsyncResponse {
                id: self.req_id,
                status,
                extensions: Default::default(),
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
}

#[derive(Debug)]
pub struct ResponseManager<S> {
    store: Arc<S>,
    sender: Arc<Sender<ResponseEvent>>,
    receiver: Arc<Mutex<Receiver<ResponseEvent>>>,
}

impl<S> ResponseManager<S>
where
    S: Store + 'static,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>) -> Self {
        let (s, r) = bounded(1000);
        Self {
            store,
            sender: Arc::new(s),
            receiver: Arc::new(Mutex::new(r)),
        }
    }
    pub fn inject_request(&self, id: RequestId, peer: PeerId, root: Cid, selector: Selector) {
        let Self { store, sender, .. } = self;
        let store = Arc::clone(store);
        let sender = Arc::clone(sender);
        spawn(async move {
            let mut builder = ResponseBuilder::new(id, peer, sender);
            if let Ok(block) = store.get(&root) {
                builder.add_block(block.clone());
                if let Ok(node) = block.ipld() {
                    let loader = BlockCallbackLoader::new(store, |block| {
                        match block {
                            Some(block) => {
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
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::mem::MemStore;
    use libipld::multihash::Code;
    use libipld::DefaultParams;

    struct TestData {
        root: Cid,
        store: Arc<MemStore<DefaultParams>>,
    }

    fn gen_data() -> TestData {
        let store = Arc::new(MemStore::<DefaultParams>::default());

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

        TestData {
            root: parent_block.cid().clone(),
            store,
        }
    }

    #[async_std::test]
    async fn test_responses() {
        let TestData { root, store } = gen_data();

        let manager = ResponseManager::new(store.clone());

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        manager.inject_request(1, PeerId::random(), root, selector);
        if let Ok(ResponseEvent::Completed(peer, msg)) =
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
