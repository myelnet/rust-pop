use super::network::{
    InboundFailure, MessageCodec, OutboundFailure, ProtocolName, RequestId as ReqResId,
    RequestResponse, RequestResponseConfig, RequestResponseEvent,
};
use super::req_mgr::RequestManager;
use super::res_mgr::ResponseBuilder;
use super::traversal::Selector;
use super::{
    Config, Extensions, GraphsyncCodec, GraphsyncEvent, GraphsyncMessage, GraphsyncProtocol,
    GraphsyncRequest, GraphsyncResponse, RequestId,
};
use blockstore::types::BlockStore;
use futures::task::{Context, Poll};
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use libp2p::core::connection::{ConnectionId, ListenerId};
use libp2p::core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::{
    DialError, IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    ProtocolsHandler,
};
use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;

pub struct GraphsyncBehaviour<S: BlockStore> {
    inner: RequestResponse<GraphsyncCodec<S::Params>>,
    request_manager: RequestManager<S>,
    pending_events: VecDeque<GraphsyncEvent>,
    store: Arc<S>,
}

impl<S: 'static + BlockStore> GraphsyncBehaviour<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(config: Config, store: Arc<S>) -> Self {
        let protocols = std::iter::once(GraphsyncProtocol);
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let inner = RequestResponse::new(GraphsyncCodec::default(), protocols, rr_config);

        Self {
            inner,
            store: store.clone(),
            pending_events: VecDeque::new(),
            request_manager: RequestManager::new(store.clone()),
        }
    }
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.inner.add_address(peer_id, addr);
    }
    pub fn request(
        &mut self,
        peer_id: PeerId,
        root: Cid,
        selector: Selector,
        extensions: Extensions,
    ) -> RequestId {
        let (req_id, msg) = self
            .request_manager
            .start_request(peer_id, root, selector, extensions);
        self.inner.send_request(&peer_id, msg);
        req_id
    }
    fn inject_outbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: ReqResId,
        error: &OutboundFailure,
    ) {
        println!(
            "graphsync outbound failure {} {} {:?}",
            peer, request_id, error
        );
    }
    fn inject_inbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: ReqResId,
        error: &InboundFailure,
    ) {
        println!("inbound failure {} {} {:?}", peer, request_id, error);
    }
    fn start_request(&mut self, peer: PeerId, request: &GraphsyncRequest) {
        let mut builder = ResponseBuilder::new(request, self.store.clone());
        while let Some(msg) = builder.next() {
            self.inner.send_response(&peer, msg);
        }
        self.inner.finish_outbound(&peer);
    }
}

impl<S: 'static + BlockStore> NetworkBehaviour for GraphsyncBehaviour<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    type ProtocolsHandler =
        <RequestResponse<GraphsyncCodec<S::Params>> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = GraphsyncEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.inner.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        self.inner.inject_connected(peer_id)
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        self.inner.inject_disconnected(peer_id);
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        failed_addressses: Option<&Vec<Multiaddr>>,
    ) {
        self.inner
            .inject_connection_established(peer_id, conn, endpoint, failed_addressses)
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        handler: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
    ) {
        let req_res = handler;
        self.inner
            .inject_connection_closed(peer_id, conn, endpoint, req_res)
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
        error: &DialError,
    ) {
        let req_res = handler;
        self.inner.inject_dial_failure(peer_id, req_res, error)
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.inner.inject_new_listen_addr(id, addr)
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.inner.inject_expired_listen_addr(id, addr)
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_new_external_addr(addr)
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_expired_external_addr(addr)
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn Error + 'static)) {
        self.inner.inject_listener_error(id, err)
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.inner.inject_listener_closed(id, reason)
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        return self.inner.inject_event(peer_id, conn, event);
    }

    fn poll(
        &mut self,
        ctx: &mut Context,
        pparams: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        // empty the pending event queue
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
        }
        while let Poll::Ready(event) = self.inner.poll(ctx, pparams) {
            let event = match event {
                NetworkBehaviourAction::GenerateEvent(event) => event,
                NetworkBehaviourAction::Dial { opts, handler } => {
                    return Poll::Ready(NetworkBehaviourAction::Dial { opts, handler });
                }
                NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    handler,
                    event,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event,
                    });
                }
                NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    });
                }
                NetworkBehaviourAction::CloseConnection {
                    peer_id,
                    connection,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                        peer_id,
                        connection,
                    });
                }
            };
            match event {
                RequestResponseEvent::Message { peer, message } => {
                    let msg = message.message;
                    if !msg.requests.is_empty() {
                        for req in msg.requests.values() {
                            self.start_request(peer, req);
                        }
                    }
                    if !msg.responses.is_empty() {
                        let mut events = self.request_manager.inject_response(&msg);
                        self.pending_events.append(&mut events);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            GraphsyncEvent::ResponseReceived(
                                peer,
                                msg.responses.values().cloned().collect(),
                            ),
                        ));
                    }
                }
                RequestResponseEvent::OutboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    self.inject_outbound_failure(&peer, request_id, &error);
                }
                RequestResponseEvent::InboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    self.inject_inbound_failure(&peer, request_id, &error);
                }
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::super::traversal::{RecursionLimit, Selector};
    use super::*;
    use async_std::task;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use futures::prelude::*;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;
    use libipld::{Block, Cid};
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use std::time::Duration;

    fn mk_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox)>) {
        let id_key = identity::Keypair::generate_ed25519();
        let peer_id = id_key.public().to_peer_id();
        let dh_key = Keypair::<X25519Spec>::new()
            .into_authentic(&id_key)
            .unwrap();
        let noise = NoiseConfig::xx(dh_key).into_authenticated();

        let transport = TcpConfig::new()
            .nodelay(true)
            .upgrade(libp2p::core::upgrade::Version::V1)
            .authenticate(noise)
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .boxed();
        (peer_id, transport)
    }

    struct Peer<B: 'static + BlockStore>
    where
        Ipld: Decode<<<B>::Params as StoreParams>::Codecs>,
    {
        peer_id: PeerId,
        addr: Multiaddr,
        store: Arc<B>,
        swarm: Swarm<GraphsyncBehaviour<B>>,
    }

    impl<B: BlockStore> Peer<B>
    where
        Ipld: Decode<<<B>::Params as StoreParams>::Codecs>,
    {
        fn new(bs: B) -> Self {
            let (peer_id, trans) = mk_transport();
            let store = Arc::new(bs);
            let mut swarm = Swarm::new(
                trans,
                GraphsyncBehaviour::new(Config::default(), store.clone()),
                peer_id,
            );
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
                store,
            }
        }

        fn add_address(&mut self, peer: &Peer<B>) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn swarm(&mut self) -> &mut Swarm<GraphsyncBehaviour<B>> {
            &mut self.swarm
        }

        fn spawn(mut self, _name: &'static str) -> PeerId {
            let peer_id = self.peer_id;
            task::spawn(async move {
                loop {
                    let event = self.swarm.next().await;
                    println!("event {:?}", event);
                }
            });
            peer_id
        }

        async fn next(&mut self) -> Option<GraphsyncEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    return Some(event);
                }
            }
        }
    }

    fn assert_response_ok(event: Option<GraphsyncEvent>, id: RequestId) {
        if let Some(GraphsyncEvent::ResponseReceived(_, responses)) = event {
            assert_eq!(responses[0].id, id);
        } else {
            panic!("{:?} is not a response event", event);
        }
    }

    fn assert_progress_ok(event: Option<GraphsyncEvent>, id: RequestId, cid: Cid, size: usize) {
        if let Some(GraphsyncEvent::Progress { req_id, link, data }) = event {
            assert_eq!(req_id, id);
            assert_eq!(link, cid);
            assert_eq!(data.len(), size);
        } else {
            panic!("{:?} is not a progress event", event);
        }
    }

    fn assert_complete_ok(event: Option<GraphsyncEvent>, id: RequestId) {
        if let Some(GraphsyncEvent::Complete(id2, Ok(()))) = event {
            assert_eq!(id2, id);
        } else {
            panic!("{:?} is not a complete event", event);
        }
    }

    #[async_std::test]
    async fn behaviour_request() {
        let peer1 = Peer::new(MemoryBlockStore::default());
        let mut peer2 = Peer::new(MemoryBlockStore::default());
        peer2.add_address(&peer1);

        let store = peer1.store.clone();
        let peer1 = peer1.spawn("peer1");

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

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let id = peer2.swarm().behaviour_mut().request(
            peer1,
            parent_block.cid().clone(),
            selector,
            Extensions::default(),
        );

        assert_response_ok(peer2.next().await, id);

        assert_progress_ok(
            peer2.next().await,
            id,
            Cid::try_from("bafyreib6ba6oakwqzsg4vv6sogb7yysu5yqqe7dqth6z3nulqkyj7lom4a").unwrap(),
            161,
        );
        assert_progress_ok(
            peer2.next().await,
            id,
            Cid::try_from("bafyreiho2e2clchrto55m3va2ygfnbc6d4bl73xldmsqvy2hjino3gxmvy").unwrap(),
            18,
        );
        assert_progress_ok(
            peer2.next().await,
            id,
            Cid::try_from("bafyreibwnmylvsglbfzglba6jvdz7b5w34p4ypecrbjrincneuskezhcq4").unwrap(),
            18,
        );
        assert_complete_ok(peer2.next().await, id);
    }
}
