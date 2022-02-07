use crate::graphsync::{GraphsyncMessage, GraphsyncRequest, Prefix, RequestId};
use crate::traversal::{AsyncLoader, Error, Progress, Selector};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::task::{Context, Poll};
use fnv::FnvHashMap;
use futures_lite::stream::StreamExt;
use libipld::codec::Decode;
use libipld::store::{Store, StoreParams};
use libipld::{Block, Cid, Ipld};
use libp2p::core::PeerId;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, Mutex,
};

#[cfg(not(target_os = "unknown"))]
use async_std::task::spawn;

#[cfg(target_os = "unknown")]
use async_std::task::spawn_local as spawn;

#[derive(Debug)]
pub enum RequestEvent {
    NewRequest(PeerId, GraphsyncMessage),
    Progress {
        req_id: RequestId,
        link: Cid,
        size: usize,
    },
    Completed(RequestId, Result<(), Error>),
}

#[derive(Debug)]
pub struct RequestManager<S: Store> {
    id_counter: Arc<AtomicI32>,
    store: Arc<S>,
    sender: Arc<Sender<RequestEvent>>,
    receiver: Arc<Mutex<Receiver<RequestEvent>>>,
    ongoing: Arc<Mutex<FnvHashMap<RequestId, Arc<Sender<Block<S::Params>>>>>>,
}

impl<S: Store + 'static> RequestManager<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>) -> Self {
        let (s, r) = bounded(1000);
        Self {
            store,
            ongoing: Default::default(),
            id_counter: Arc::new(AtomicI32::new(1)),
            sender: Arc::new(s),
            receiver: Arc::new(Mutex::new(r)),
        }
    }
    pub fn start_request(&self, responder: PeerId, root: Cid, selector: Selector) -> RequestId {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let store = self.store.clone();
        let sender = self.sender.clone();
        let sel = selector.clone();
        let loader = AsyncLoader::new(store, move |blk| {
            sender
                .try_send(RequestEvent::Progress {
                    req_id: id,
                    link: blk.cid().clone(),
                    size: blk.data().len(),
                })
                .map_err(|e| e.to_string())?;
            Ok(())
        });
        let sender = self.sender.clone();
        self.ongoing.lock().unwrap().insert(id, loader.sender());
        spawn(async move {
            let mut progress = Progress::new(loader);
            let result = progress
                .walk_adv(&Ipld::Link(root), selector, &|_, _| Ok(()))
                .await;
            sender
                .try_send(RequestEvent::Completed(id, result))
                .unwrap();
        });
        let mut msg = GraphsyncMessage::default();
        msg.requests.insert(
            id,
            GraphsyncRequest {
                id,
                root,
                selector: sel,
                extensions: Default::default(),
            },
        );
        self.sender
            .try_send(RequestEvent::NewRequest(responder, msg))
            .unwrap();
        id
    }
    pub fn inject_response(&self, msg: GraphsyncMessage) {
        for (prefix, data) in msg.blocks {
            if let Ok(prefix) = Prefix::new_from_bytes(&prefix) {
                if let Ok(cid) = prefix.to_cid(&data) {
                    let block = Block::new_unchecked(cid, data);
                    for (_, res) in msg.responses.iter() {
                        if let Some(sender) = self.ongoing.lock().unwrap().get(&res.id) {
                            sender.try_send(block.clone()).unwrap();
                        }
                    }
                }
            }
        }
    }
    pub fn next(&self, ctx: &mut Context) -> Poll<Option<RequestEvent>> {
        self.receiver.lock().unwrap().poll_next(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphsync::{GraphsyncResponse, ResponseStatusCode};
    use crate::traversal::RecursionLimit;
    use async_std::channel::RecvError;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::mem::MemStore;
    use libipld::multihash::Code;
    use libipld::DefaultParams;

    fn assert_progress_ok(
        result: Result<RequestEvent, RecvError>,
        id: RequestId,
        size2: usize,
        cid: Cid,
    ) {
        if let Ok(evt) = result {
            match evt {
                RequestEvent::Progress { req_id, size, link } => {
                    assert_eq!(req_id, id);
                    assert_eq!(size, size2);
                    assert_eq!(link, cid,);
                }
                _ => panic!("Received wrong event"),
            }
        } else {
            panic!("Receiver is broken");
        }
    }

    #[async_std::test]
    async fn test_start_request() {
        let store = Arc::new(MemStore::<DefaultParams>::default());

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block =
            Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();

        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block =
            Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();

        let parent = ipld!({
            "children": [leaf1_block.cid(), leaf2_block.cid()],
            "favouriteChild": leaf2_block.cid(),
            "name": "parent",
        });
        let parent_block =
            Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &parent).unwrap();
        let root = parent_block.cid().clone();
        let blocks: Vec<(Cid, Vec<u8>)> = vec![
            leaf1_block.into_inner(),
            leaf2_block.into_inner(),
            parent_block.into_inner(),
        ];

        let manager = RequestManager::new(store.clone());

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        // Start the request and traversal
        manager.start_request(PeerId::random(), root, selector);
        // we should receive an outbound message to send over the network
        if let Ok(evt) = manager.receiver.lock().unwrap().recv().await {
            match evt {
                RequestEvent::NewRequest(poer, msg) => {
                    assert_eq!(msg.requests.len(), 1);
                }
                _ => panic!("Received wrong event"),
            }
        } else {
            panic!("receiver is broken");
        }

        // simulate a response containing all the blocks
        let mut response = GraphsyncMessage::default();
        response.responses.insert(
            1,
            GraphsyncResponse {
                id: 1,
                status: ResponseStatusCode::RequestCompletedFull,
                extensions: Default::default(),
            },
        );
        for (cid, data) in blocks.iter() {
            response
                .blocks
                .push((Prefix::from(*cid).to_bytes(), data.to_vec()));
        }
        manager.inject_response(response);
        // We receive the first block
        assert_progress_ok(
            manager.receiver.lock().unwrap().recv().await,
            1,
            161,
            Cid::try_from("bafyreib6ba6oakwqzsg4vv6sogb7yysu5yqqe7dqth6z3nulqkyj7lom4a").unwrap(),
        );
        // We receive the second block
        assert_progress_ok(
            manager.receiver.lock().unwrap().recv().await,
            1,
            18,
            Cid::try_from("bafyreiho2e2clchrto55m3va2ygfnbc6d4bl73xldmsqvy2hjino3gxmvy").unwrap(),
        );
        // We receive the last block
        assert_progress_ok(
            manager.receiver.lock().unwrap().recv().await,
            1,
            18,
            Cid::try_from("bafyreibwnmylvsglbfzglba6jvdz7b5w34p4ypecrbjrincneuskezhcq4").unwrap(),
        );
        // the traversal should complete and we should receive the result
        if let Ok(evt) = manager.receiver.lock().unwrap().recv().await {
            match evt {
                RequestEvent::Completed(id, result) => {
                    assert_eq!(id, 1);
                    assert_eq!(Ok(()), result);
                }
                _ => panic!("Received wrong event"),
            }
        } else {
            panic!("Receiver is broken");
        };
    }
}
