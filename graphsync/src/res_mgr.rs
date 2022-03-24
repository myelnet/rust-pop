use super::traversal::{BlockIterator, Error, Selector};
use super::{
    Extensions, GraphSyncBlock, GraphSyncLinkData, GraphSyncMessage, GraphSyncResponse, LinkAction,
    Prefix, RequestId, ResponseStatusCode,
};
use blockstore::types::BlockStore;
use filecoin::cid_helpers::CidCbor;
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{ipld::Ipld, Cid};
use std::mem;
use std::sync::Arc;

pub struct ResponseBuilder<S> {
    pub paused: bool,
    pub cancelled: bool,
    pub rejected: bool,
    pub status: ResponseStatusCode,
    req_id: RequestId,
    it: BlockIterator<S>,
    max_size: usize,
    partial: bool,
    error: Option<Error>,
    extensions: Extensions,
}

impl<S> Iterator for ResponseBuilder<S>
where
    S: BlockStore,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    type Item = GraphSyncMessage;

    fn next(&mut self) -> Option<Self::Item> {
        // if the status was set to final this iterator is done and will only return None.
        match self.status {
            ResponseStatusCode::RequestCompletedFull
            | ResponseStatusCode::RequestCompletedPartial
            | ResponseStatusCode::RequestFailedUnknown
            | ResponseStatusCode::RequestPaused
            | ResponseStatusCode::RequestCancelled
            | ResponseStatusCode::RequestFailedContentNotFound
            | ResponseStatusCode::RequestRejected => return None,
            _ => {}
        };

        if self.paused {
            self.status = ResponseStatusCode::RequestPaused;
        }
        if self.cancelled {
            self.status = ResponseStatusCode::RequestCancelled;
        }
        if self.rejected {
            self.status = ResponseStatusCode::RequestRejected;
        }
        if self.paused | self.cancelled | self.rejected {
            return Some(
                GraphSyncResponse {
                    id: self.req_id,
                    status: self.status,
                    metadata: None,
                    extensions: None,
                }
                .into(),
            );
        }

        let mut size = 0;
        let mut metadata = Vec::new();
        let mut blocks = Vec::new();
        // iterate until we've filled the message to capacity
        while size < self.max_size {
            match self.it.next() {
                Some(Ok(block)) => {
                    size += block.data().len();
                    let (cid, data) = block.into_inner();
                    blocks.push(GraphSyncBlock {
                        prefix: Prefix::from(cid).to_bytes(),
                        data,
                    });
                    // at least one block was yielded so the response can be partial
                    self.status = ResponseStatusCode::PartialResponse;
                    metadata.push(GraphSyncLinkData {
                        link: CidCbor::from(cid),
                        action: LinkAction::Present,
                    });
                }
                // we found a missing block but more might still be coming
                Some(Err(Error::BlockNotFound(cid))) => {
                    self.partial = true;
                    metadata.push(GraphSyncLinkData {
                        link: CidCbor::from(cid),
                        action: LinkAction::Missing,
                    });
                }
                Some(Err(e)) => {
                    self.status = ResponseStatusCode::RequestFailedUnknown;
                    self.error = Some(e);
                }
                None => {
                    self.status = match self.status {
                        // If we end the traversal and no content was ever returned, the request
                        // failed. TODO: we should be able to pass an error message somewhere in
                        // the extensions maybe?
                        ResponseStatusCode::RequestAcknowledged => {
                            if self.partial {
                                ResponseStatusCode::RequestFailedContentNotFound
                            } else {
                                ResponseStatusCode::RequestFailedUnknown
                            }
                        }
                        // in this case we've yielded some blocks
                        ResponseStatusCode::PartialResponse => {
                            if self.partial {
                                ResponseStatusCode::RequestCompletedPartial
                            } else {
                                ResponseStatusCode::RequestCompletedFull
                            }
                        }
                        // no change
                        status => status,
                    };
                    break;
                }
            }
        }

        let extensions = mem::take(&mut self.extensions);
        let mut response = GraphSyncResponse {
            id: self.req_id,
            status: self.status,
            metadata: None,
            extensions: None,
        };
        if !metadata.is_empty() {
            response.metadata = Some(metadata);
        }
        if !extensions.is_empty() {
            response.extensions = Some(extensions);
        }
        Some(GraphSyncMessage::from_blocks(response, blocks))
    }
}

impl<S> ResponseBuilder<S>
where
    S: BlockStore,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(id: RequestId, root: Cid, selector: Selector, store: Arc<S>) -> Self {
        Self {
            req_id: id,
            partial: false,
            max_size: 500 * 1024,
            extensions: Default::default(),
            paused: false,
            cancelled: false,
            rejected: false,
            error: None,
            // no response messages have been created yet
            status: ResponseStatusCode::RequestAcknowledged,
            // we tell the block iterator not to follow the same link twice as it was already sent.
            it: BlockIterator::new(store, root, selector).ignore_duplicate_links(),
        }
    }
    /// set custom extensions to be sent with the first message of the iterator.
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions.extend(extensions);
        self
    }
    /// pause the iterator and send the status in the next response.
    pub fn pause(mut self) -> Self {
        self.paused = true;
        self
    }
    /// cancel the iterator and send the status in the last response.
    pub fn cancel(mut self) -> Self {
        self.cancelled = true;
        self
    }
    /// cancel the iterator and send the rejected status in the last response.
    pub fn reject(mut self) -> Self {
        self.rejected = true;
        self
    }
    pub fn is_completed(&self) -> bool {
        match self.status {
            ResponseStatusCode::RequestCompletedFull
            | ResponseStatusCode::RequestCompletedPartial => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traversal::{RecursionLimit, Selector};
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use libipld::{ipld, Block, Cid};
    use uuid::Uuid;

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

    #[test]
    fn response_builder() {
        let TestData { root, store } = gen_data();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let mut builder = ResponseBuilder::new(Uuid::new_v4(), root, selector, store.clone());

        let GraphSyncMessage::V2 {
            responses, blocks, ..
        } = builder.next().unwrap();
        assert_eq!(responses.as_ref().unwrap().len(), 1);
        assert_eq!(blocks.as_ref().unwrap().len(), 3);
        assert_eq!(
            responses.as_ref().unwrap()[0].status,
            ResponseStatusCode::RequestCompletedFull
        );
        let end = builder.next();
        assert_eq!(end, None);
    }

    #[test]
    fn response_builder_missing_block() {
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

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let mut builder = ResponseBuilder::new(
            Uuid::new_v4(),
            parent_block.cid().clone(),
            selector,
            store.clone(),
        );
        let GraphSyncMessage::V2 {
            responses, blocks, ..
        } = builder.next().unwrap();
        assert_eq!(responses.as_ref().unwrap().len(), 1);
        assert_eq!(blocks.as_ref().unwrap().len(), 2);
        assert_eq!(
            responses.as_ref().unwrap()[0].status,
            ResponseStatusCode::RequestCompletedPartial
        );

        responses.as_ref().unwrap()[0]
            .metadata
            .as_ref()
            .unwrap()
            .iter()
            .find(|item| item.action == LinkAction::Missing)
            .unwrap();
        let end = builder.next();
        assert_eq!(end, None);
    }
}
