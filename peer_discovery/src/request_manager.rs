use super::{DiscoveryRequest, RequestId};
use async_std::channel::{bounded, Receiver, Sender};
use async_std::task::{Context, Poll};
use futures_lite::stream::StreamExt;
use libp2p::core::PeerId;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, Mutex,
};

#[derive(Debug)]
pub enum RequestEvent {
    NewRequest(PeerId, DiscoveryRequest),
}

#[derive(Debug)]
pub struct RequestManager {
    id_counter: Arc<AtomicI32>,
    sender: Arc<Sender<RequestEvent>>,
    receiver: Arc<Mutex<Receiver<RequestEvent>>>,
}

impl RequestManager {
    pub fn new() -> Self {
        let (s, r) = bounded(1000);
        Self {
            id_counter: Arc::new(AtomicI32::new(1)),
            sender: Arc::new(s),
            receiver: Arc::new(Mutex::new(r)),
        }
    }
    pub fn start_request(&self, responder: PeerId) -> RequestId {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        // self.ongoing.lock().unwrap().insert(id, sender);
        let msg = DiscoveryRequest { id };
        self.sender
            .try_send(RequestEvent::NewRequest(responder, msg))
            .unwrap();
        id
    }
    pub fn next(&self, ctx: &mut Context) -> Poll<Option<RequestEvent>> {
        self.receiver.lock().unwrap().poll_next(ctx)
    }
}
