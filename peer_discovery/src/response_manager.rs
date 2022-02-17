use super::{
    DiscoveryHooks, DiscoveryRequest, DiscoveryResponse, PeerTable, RequestId, ResponseStatusCode,
    SerializablePeerTable,
};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::task::{Context, Poll};
use futures_lite::stream::StreamExt;
use libp2p::core::PeerId;
use libp2p::request_response::ResponseChannel;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug)]
pub enum ResponseEvent {
    Accepted(PeerId, DiscoveryRequest),
    Completed(
        PeerId,
        ResponseChannel<DiscoveryResponse>,
        DiscoveryResponse,
    ),
}

#[derive(Debug)]
pub struct ResponseBuilder {
    req_id: RequestId,
    peer_id: PeerId,
    sender: Arc<Sender<ResponseEvent>>,
    channel: ResponseChannel<DiscoveryResponse>,
}

impl ResponseBuilder {
    pub fn new(
        req_id: RequestId,
        peer_id: PeerId,
        sender: Arc<Sender<ResponseEvent>>,
        channel: ResponseChannel<DiscoveryResponse>,
    ) -> Self {
        Self {
            req_id,
            peer_id,
            sender,
            channel,
        }
    }

    pub fn completed(self, addresses: SerializablePeerTable) {
        self.send(ResponseStatusCode::RequestCompletedFull, Some(addresses));
    }
    pub fn failed(self, _err: String) {
        self.send(ResponseStatusCode::RequestFailedUnknown, None);
    }
    pub fn not_found(self) {
        self.send(ResponseStatusCode::RequestFailedPeersNotFound, None);
    }
    pub fn rejected(self) {
        self.send(ResponseStatusCode::RequestRejected, None);
    }
    fn send(self, status: ResponseStatusCode, addresses: Option<SerializablePeerTable>) {
        let msg = DiscoveryResponse {
            id: self.req_id,
            status,
            addresses,
        };

        let event = ResponseEvent::Completed(self.peer_id, self.channel, msg);

        self.sender.try_send(event).unwrap()
    }
}

pub struct ResponseManager {
    sender: Arc<Sender<ResponseEvent>>,
    receiver: Arc<Mutex<Receiver<ResponseEvent>>>,
    hooks: Arc<RwLock<DiscoveryHooks>>,
}

impl ResponseManager {
    pub fn new(hooks: Arc<RwLock<DiscoveryHooks>>) -> Self {
        let (s, r) = bounded(1000);
        Self {
            sender: Arc::new(s),
            receiver: Arc::new(Mutex::new(r)),
            hooks,
        }
    }
    pub fn inject_request(
        &self,
        peer: PeerId,
        request: &DiscoveryRequest,
        channel: ResponseChannel<DiscoveryResponse>,
        addresses: PeerTable,
    ) {
        let Self { sender, hooks, .. } = self;
        let sender = Arc::clone(sender);
        let id = request.id;
        let builder = ResponseBuilder::new(id, peer, sender.clone(), channel);
        // validate request and add any extensions
        let approved = (hooks.read().unwrap().incoming_request)(&peer);
        if !approved {
            builder.rejected();
            return;
        }
        if addresses.keys().len() == 0 {
            builder.not_found();
            return;
        }
        sender
            .try_send(ResponseEvent::Accepted(peer, request.clone()))
            .unwrap();

        let new_addresses: HashMap<Vec<u8>, Vec<Vec<u8>>> = addresses
            .iter()
            .map_while(|(peer, addresses)| {
                let mut addr_vec = Vec::new();
                for addr in addresses {
                    addr_vec.push((*addr).to_vec())
                }
                Some((peer.to_bytes(), addr_vec))
            })
            .collect();
        builder.completed(new_addresses)
    }

    pub fn next(&self, ctx: &mut Context) -> Poll<Option<ResponseEvent>> {
        self.receiver.lock().unwrap().poll_next(ctx)
    }
}
