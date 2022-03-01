use super::traversal::{BlockIterator, Error, Selector};
use super::{Extensions, GraphsyncEvent, GraphsyncMessage, GraphsyncRequest, Prefix, RequestId};
use blockstore::temp::TempBlockStore;
use blockstore::types::BlockStore;
use fnv::FnvHashMap;
use libipld::{codec::Decode, store::StoreParams, Block, Cid, Ipld};
use libp2p::core::PeerId;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, Mutex,
};

pub struct RequestManager<S: BlockStore> {
    id_counter: Arc<AtomicI32>,
    ongoing: Arc<Mutex<FnvHashMap<RequestId, BlockIterator<TempBlockStore<S>>>>>,
    store: Arc<TempBlockStore<S>>,
}

impl<S: BlockStore> RequestManager<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: Arc::new(TempBlockStore::new(store)),
            ongoing: Default::default(),
            id_counter: Arc::new(AtomicI32::new(1)),
        }
    }
    pub fn start_request(
        &self,
        responder: PeerId,
        root: Cid,
        selector: Selector,
        extensions: Extensions,
    ) -> (RequestId, GraphsyncMessage) {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let it =
            BlockIterator::new(self.store.clone(), root, selector.clone()).ignore_duplicate_links();
        self.ongoing.lock().unwrap().insert(id, it);
        let mut msg = GraphsyncMessage::default();
        msg.requests.insert(
            id,
            GraphsyncRequest {
                id,
                root,
                selector,
                extensions,
            },
        );
        (id, msg)
    }
    /// advance traversals and return a queue of events
    pub fn inject_response(&self, msg: &GraphsyncMessage) -> VecDeque<GraphsyncEvent> {
        // first insert all the blocks in the pending blockstore
        msg.blocks
            .iter()
            .map_while(|(prefix, data)| {
                let prefix = Prefix::new_from_bytes(prefix).ok()?;
                let cid = prefix.to_cid(data).ok()?;
                Some(Block::new_unchecked(cid, data.to_vec()))
            })
            .for_each(|blk| self.store.insert(&blk).unwrap());
        // then move forward all the traversals
        msg.responses
            .iter()
            .fold(VecDeque::new(), |mut queue, (_, res)| {
                if let Some(it) = self.ongoing.lock().unwrap().get_mut(&res.id) {
                    loop {
                        match it.next() {
                            Some(Ok(blk)) => {
                                queue.push_back(GraphsyncEvent::Progress {
                                    req_id: res.id,
                                    link: *blk.cid(),
                                    data: blk.data().to_vec(),
                                });
                            }
                            None => {
                                queue.push_back(GraphsyncEvent::Complete(res.id, Ok(())));
                                break;
                            }
                            _ => {
                                break;
                            }
                        }
                    }
                }
                queue
            })
    }
}

#[cfg(test)]
mod tests {
    use super::super::traversal::RecursionLimit;
    use super::super::{GraphsyncResponse, ResponseStatusCode};
    use super::*;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;
    use libipld::DefaultParams;

    #[test]
    fn test_req_mgr() {
        let store = Arc::new(MemoryBlockStore::default());

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
        let (req_id, msg) =
            manager.start_request(PeerId::random(), root, selector, Extensions::default());
        // we should receive an outbound message to send over the network
        assert_eq!(msg.requests.len(), 1);

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
        let results = manager.inject_response(&response);
        let expected = vec![
            Cid::try_from("bafyreib6ba6oakwqzsg4vv6sogb7yysu5yqqe7dqth6z3nulqkyj7lom4a").unwrap(),
            Cid::try_from("bafyreiho2e2clchrto55m3va2ygfnbc6d4bl73xldmsqvy2hjino3gxmvy").unwrap(),
            Cid::try_from("bafyreibwnmylvsglbfzglba6jvdz7b5w34p4ypecrbjrincneuskezhcq4").unwrap(),
        ];
        for (i, evt) in results.iter().enumerate() {
            match evt {
                GraphsyncEvent::Progress { link, .. } => {
                    assert_eq!(*link, expected[i]);
                }
                e => {
                    assert_eq!(e, &GraphsyncEvent::Complete(1, Ok(())));
                }
            }
        }
    }
}
