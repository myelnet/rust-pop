#[cfg(feature = "compat")]
use crate::compat::{CompatMessage, CompatProtocol, InboundMessage};
use crate::protocol::{
    BitswapCodec, BitswapProtocol, BitswapRequest, BitswapResponse, RequestType,
};
use crate::query::{QueryEvent, QueryId, QueryManager, Request, Response};
use crate::stats::*;
use blockstore::types::BlockStore;
use fnv::FnvHashMap;
#[cfg(feature = "compat")]
use fnv::FnvHashSet;
use futures::channel::mpsc;
use futures::stream::{Stream, StreamExt};
use futures::task::{Context, Poll};
use libipld::error::BlockNotFound;
use libipld::store::StoreParams;
use libipld::{Block, Cid, Result};
use libp2p::core::connection::{ConnectionId, ListenerId};
#[cfg(feature = "compat")]
use libp2p::core::either::EitherOutput;
use libp2p::core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p::request_response::{
    InboundFailure, OutboundFailure, ProtocolSupport, RequestId, RequestResponse,
    RequestResponseConfig, RequestResponseEvent, RequestResponseMessage, ResponseChannel,
};
use libp2p::swarm::{
    DialError, IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    ProtocolsHandler,
};
#[cfg(feature = "compat")]
use libp2p::swarm::{NotifyHandler, OneShotHandler, ProtocolsHandlerSelect};
use prometheus::Registry;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// Bitswap response channel.
pub type Channel = ResponseChannel<BitswapResponse>;

/// Event emitted by the bitswap behaviour.
#[derive(Debug)]
pub enum BitswapEvent {
    /// Received a block from a peer. Includes the number of known missing blocks for a
    /// sync query. When a block is received and missing blocks is not empty the counter
    /// is increased. If missing blocks is empty the counter is decremented.
    Progress(QueryId, usize),
    /// Received a block.
    Block(Vec<u8>),
    /// A get or sync query completed.
    Complete(QueryId, Result<()>),
}

/// Bitswap configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitswapConfig {
    /// Timeout of a request.
    pub request_timeout: Duration,
    /// Time a connection is kept alive.
    pub connection_keep_alive: Duration,
}

impl BitswapConfig {
    /// Creates a new `BitswapConfig`.
    pub fn new() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
            connection_keep_alive: Duration::from_secs(10),
        }
    }
}

impl Default for BitswapConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BitswapId {
    Bitswap(RequestId),
    #[cfg(feature = "compat")]
    Compat(Cid),
}

enum BitswapChannel {
    Bitswap(Channel),
    #[cfg(feature = "compat")]
    Compat(PeerId, Cid),
}

/// Network behaviour that handles sending and receiving blocks.
pub struct Bitswap<S: 'static + BlockStore> {
    /// Inner behaviour.
    inner: RequestResponse<BitswapCodec<S::Params>>,
    /// Query manager.
    query_manager: QueryManager,
    /// Requests.
    requests: FnvHashMap<BitswapId, QueryId>,
    /// Db request channel.
    db_tx: mpsc::UnboundedSender<DbRequest<S::Params>>,
    /// Db response channel.
    db_rx: mpsc::UnboundedReceiver<DbResponse>,
    /// Compat peers.
    #[cfg(feature = "compat")]
    compat: FnvHashSet<PeerId>,
}

impl<S: 'static + BlockStore> Bitswap<S> {
    /// Creates a new `Bitswap` behaviour.
    pub fn new(config: BitswapConfig, store: Arc<S>) -> Self
    where
        libipld::Ipld: libipld::codec::References<
            <<S as blockstore::types::BlockStore>::Params as StoreParams>::Codecs,
        >,
    {
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let protocols = std::iter::once((BitswapProtocol, ProtocolSupport::Full));
        let inner =
            RequestResponse::new(BitswapCodec::<S::Params>::default(), protocols, rr_config);
        let (db_tx, db_rx) = start_db_thread(store);
        Self {
            inner,
            query_manager: Default::default(),
            requests: Default::default(),
            db_tx,
            db_rx,
            #[cfg(feature = "compat")]
            compat: Default::default(),
        }
    }

    /// Adds an address for a peer.
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.inner.add_address(peer_id, addr);
    }

    /// Removes an address for a peer.
    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) {
        self.inner.remove_address(peer_id, addr);
    }

    /// Starts a get query with an initial guess of providers.
    pub fn get(&mut self, cid: Cid, peers: impl Iterator<Item = PeerId>) -> QueryId {
        self.query_manager.get(None, cid, peers)
    }

    /// Starts a sync query with an the initial set of missing blocks.
    pub fn sync(
        &mut self,
        cid: Cid,
        peers: Vec<PeerId>,
        missing: impl Iterator<Item = Cid>,
    ) -> QueryId {
        self.query_manager.sync(cid, peers, missing)
    }

    /// Cancels an in progress query. Returns true if a query was cancelled.
    pub fn cancel(&mut self, id: QueryId) -> bool {
        let res = self.query_manager.cancel(id);
        if res {
            REQUESTS_CANCELED.inc();
        }
        res
    }

    /// Registers prometheus metrics.
    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(REQUESTS_TOTAL.clone()))?;
        registry.register(Box::new(REQUEST_DURATION_SECONDS.clone()))?;
        registry.register(Box::new(REQUESTS_CANCELED.clone()))?;
        registry.register(Box::new(BLOCK_NOT_FOUND.clone()))?;
        registry.register(Box::new(PROVIDERS_TOTAL.clone()))?;
        registry.register(Box::new(MISSING_BLOCKS_TOTAL.clone()))?;
        registry.register(Box::new(RECEIVED_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(RECEIVED_INVALID_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(SENT_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(RESPONSES_TOTAL.clone()))?;
        registry.register(Box::new(THROTTLED_INBOUND.clone()))?;
        registry.register(Box::new(THROTTLED_OUTBOUND.clone()))?;
        registry.register(Box::new(OUTBOUND_FAILURE.clone()))?;
        registry.register(Box::new(INBOUND_FAILURE.clone()))?;
        Ok(())
    }
}

enum DbRequest<P: StoreParams> {
    Bitswap(BitswapChannel, BitswapRequest),
    Insert(Block<P>),
    MissingBlocks(QueryId, Cid),
}

enum DbResponse {
    Bitswap(BitswapChannel, BitswapResponse),
    Block(Vec<u8>),
    MissingBlocks(QueryId, Result<Vec<Cid>, blockstore::errors::Error>),
}

fn start_db_thread<S: 'static + BlockStore>(
    store: Arc<S>,
) -> (
    mpsc::UnboundedSender<DbRequest<S::Params>>,
    mpsc::UnboundedReceiver<DbResponse>,
)
where
    libipld::Ipld: libipld::codec::References<
        <<S as blockstore::types::BlockStore>::Params as StoreParams>::Codecs,
    >,
{
    let (tx, requests) = mpsc::unbounded();
    let (responses, rx) = mpsc::unbounded();
    std::thread::spawn(move || {
        let mut requests: mpsc::UnboundedReceiver<DbRequest<S::Params>> = requests;
        while let Some(request) = futures::executor::block_on(requests.next()) {
            match request {
                DbRequest::Bitswap(channel, request) => {
                    let response = match request.ty {
                        RequestType::Have => {
                            let have = store.contains(&request.cid).ok().unwrap_or_default();
                            if have {
                                RESPONSES_TOTAL.with_label_values(&["have"]).inc();
                            } else {
                                RESPONSES_TOTAL.with_label_values(&["dont_have"]).inc();
                            }
                            tracing::trace!("have {}", have);
                            BitswapResponse::Have(have)
                        }
                        RequestType::Block => match store.get(&request.cid) {
                            Ok(data) => {
                                let bytes = data.data();
                                RESPONSES_TOTAL.with_label_values(&["block"]).inc();
                                SENT_BLOCK_BYTES.inc_by(bytes.len() as u64);
                                tracing::trace!("block {}", bytes.len());
                                BitswapResponse::Block(bytes.to_vec())
                            }
                            Err(_) => {
                                println!("dont have");
                                RESPONSES_TOTAL.with_label_values(&["dont_have"]).inc();
                                tracing::trace!("have false");
                                BitswapResponse::Have(false)
                            }
                        },
                    };
                    responses
                        .unbounded_send(DbResponse::Bitswap(channel, response))
                        .ok();
                }
                DbRequest::Insert(block) => {
                    if let Err(err) = store.insert(&block) {
                        tracing::error!("error inserting blocks {}", err);
                    }
                    responses
                        .unbounded_send(DbResponse::Block(block.data().to_vec()))
                        .ok();
                }
                DbRequest::MissingBlocks(id, cid) => {
                    let res = store.missing_blocks(&cid);
                    responses
                        .unbounded_send(DbResponse::MissingBlocks(id, res))
                        .ok();
                }
            }
        }
    });
    (tx, rx)
}

impl<S: 'static + BlockStore> Bitswap<S> {
    /// Processes an incoming bitswap request.
    fn inject_request(&mut self, channel: BitswapChannel, request: BitswapRequest) {
        self.db_tx
            .unbounded_send(DbRequest::Bitswap(channel, request))
            .ok();
    }

    /// Processes an incoming bitswap response.
    fn inject_response(&mut self, id: BitswapId, peer: PeerId, response: BitswapResponse) {
        if let Some(id) = self.requests.remove(&id) {
            match response {
                BitswapResponse::Have(have) => {
                    self.query_manager
                        .inject_response(id, Response::Have(peer, have));
                }
                BitswapResponse::Block(data) => {
                    if let Some(info) = self.query_manager.query_info(id) {
                        let len = data.len();
                        if let Ok(block) = Block::new(info.cid, data) {
                            RECEIVED_BLOCK_BYTES.inc_by(len as u64);
                            self.db_tx.unbounded_send(DbRequest::Insert(block)).ok();
                            self.query_manager
                                .inject_response(id, Response::Block(peer, true));
                        } else {
                            tracing::error!("received invalid block");
                            RECEIVED_INVALID_BLOCK_BYTES.inc_by(len as u64);
                            self.query_manager
                                .inject_response(id, Response::Block(peer, false));
                        }
                    }
                }
            }
        }
    }

    fn inject_outbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: RequestId,
        error: &OutboundFailure,
    ) {
        tracing::debug!(
            "bitswap outbound failure {} {} {:?}",
            peer,
            request_id,
            error
        );
        match error {
            OutboundFailure::DialFailure => {
                OUTBOUND_FAILURE.with_label_values(&["dial_failure"]).inc();
            }
            OutboundFailure::Timeout => {
                OUTBOUND_FAILURE.with_label_values(&["timeout"]).inc();
            }
            OutboundFailure::ConnectionClosed => {
                OUTBOUND_FAILURE
                    .with_label_values(&["connection_closed"])
                    .inc();
            }
            OutboundFailure::UnsupportedProtocols => {
                OUTBOUND_FAILURE
                    .with_label_values(&["unsupported_protocols"])
                    .inc();
            }
        }
    }

    fn inject_inbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: RequestId,
        error: &InboundFailure,
    ) {
        tracing::error!(
            "bitswap inbound failure {} {} {:?}",
            peer,
            request_id,
            error
        );
        match error {
            InboundFailure::Timeout => {
                INBOUND_FAILURE.with_label_values(&["timeout"]).inc();
            }
            InboundFailure::ConnectionClosed => {
                INBOUND_FAILURE
                    .with_label_values(&["connection_closed"])
                    .inc();
            }
            InboundFailure::UnsupportedProtocols => {
                INBOUND_FAILURE
                    .with_label_values(&["unsupported_protocols"])
                    .inc();
            }
            InboundFailure::ResponseOmission => {
                INBOUND_FAILURE
                    .with_label_values(&["response_omission"])
                    .inc();
            }
        }
    }
}

impl<S: 'static + BlockStore> NetworkBehaviour for Bitswap<S> {
    #[cfg(not(feature = "compat"))]
    type ProtocolsHandler =
        <RequestResponse<BitswapCodec<S::Params>> as NetworkBehaviour>::ProtocolsHandler;

    #[cfg(feature = "compat")]
    #[allow(clippy::type_complexity)]
    type ProtocolsHandler = ProtocolsHandlerSelect<
        <RequestResponse<BitswapCodec<P>> as NetworkBehaviour>::ProtocolsHandler,
        OneShotHandler<CompatProtocol, CompatMessage, InboundMessage>,
    >;
    type OutEvent = BitswapEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        #[cfg(not(feature = "compat"))]
        return self.inner.new_handler();
        #[cfg(feature = "compat")]
        ProtocolsHandler::select(self.inner.new_handler(), OneShotHandler::default())
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        self.inner.inject_connected(peer_id)
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        self.inner.inject_disconnected(peer_id);
        #[cfg(feature = "compat")]
        self.compat.remove(peer_id);
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
        #[cfg(feature = "compat")]
        let (req_res, _oneshot) = handler.into_inner();
        #[cfg(not(feature = "compat"))]
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
        #[cfg(feature = "compat")]
        let (req_res, _oneshot) = handler.into_inner();
        #[cfg(not(feature = "compat"))]
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
        #[cfg(not(feature = "compat"))]
        return self.inner.inject_event(peer_id, conn, event);
        #[cfg(feature = "compat")]
        match event {
            EitherOutput::First(event) => self.inner.inject_event(peer_id, conn, event),
            EitherOutput::Second(msg) => {
                for msg in msg.0 {
                    match msg {
                        CompatMessage::Request(req) => {
                            tracing::trace!("received compat request");
                            self.inject_request(BitswapChannel::Compat(peer_id, req.cid), req);
                        }
                        CompatMessage::Response(cid, res) => {
                            tracing::trace!("received compat response");
                            self.inject_response(BitswapId::Compat(cid), peer_id, res);
                        }
                    }
                }
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context,
        pp: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        let mut exit = false;
        while !exit {
            exit = true;
            while let Poll::Ready(Some(response)) = Pin::new(&mut self.db_rx).poll_next(cx) {
                exit = false;
                match response {
                    DbResponse::Bitswap(channel, response) => match channel {
                        BitswapChannel::Bitswap(channel) => {
                            self.inner.send_response(channel, response).ok();
                        }
                        #[cfg(feature = "compat")]
                        BitswapChannel::Compat(peer_id, cid) => {
                            let compat = CompatMessage::Response(cid, response);
                            return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                                peer_id,
                                handler: NotifyHandler::Any,
                                event: EitherOutput::Second(compat),
                            });
                        }
                    },
                    DbResponse::MissingBlocks(id, res) => match res {
                        Ok(missing) => {
                            MISSING_BLOCKS_TOTAL.inc_by(missing.len() as u64);
                            self.query_manager
                                .inject_response(id, Response::MissingBlocks(missing));
                        }
                        Err(err) => {
                            self.query_manager.cancel(id);
                            let event = BitswapEvent::Complete(id, Err(err.into()));
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                        }
                    },
                    DbResponse::Block(block) => {
                        let event = BitswapEvent::Block(block);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                }
            }
            while let Some(query) = self.query_manager.next() {
                exit = false;
                match query {
                    QueryEvent::Request(id, req) => match req {
                        Request::Have(peer_id, cid) => {
                            let req = BitswapRequest {
                                ty: RequestType::Have,
                                cid,
                            };
                            let rid = self.inner.send_request(&peer_id, req);
                            self.requests.insert(BitswapId::Bitswap(rid), id);
                        }
                        Request::Block(peer_id, cid) => {
                            let req = BitswapRequest {
                                ty: RequestType::Block,
                                cid,
                            };
                            let rid = self.inner.send_request(&peer_id, req);
                            self.requests.insert(BitswapId::Bitswap(rid), id);
                        }
                        Request::MissingBlocks(cid) => {
                            self.db_tx
                                .unbounded_send(DbRequest::MissingBlocks(id, cid))
                                .ok();
                        }
                    },
                    QueryEvent::Progress(id, missing) => {
                        let event = BitswapEvent::Progress(id, missing);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                    QueryEvent::Complete(id, res) => {
                        if res.is_err() {
                            BLOCK_NOT_FOUND.inc();
                        }
                        let event = BitswapEvent::Complete(
                            id,
                            res.map_err(|cid| BlockNotFound(cid).into()),
                        );
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                }
            }
            while let Poll::Ready(event) = self.inner.poll(cx, pp) {
                exit = false;
                let event = match event {
                    NetworkBehaviourAction::GenerateEvent(event) => event,
                    NetworkBehaviourAction::Dial { opts, handler } => {
                        #[cfg(feature = "compat")]
                        let handler = ProtocolsHandler::select(handler, Default::default());
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
                            #[cfg(not(feature = "compat"))]
                            event,
                            #[cfg(feature = "compat")]
                            event: EitherOutput::First(event),
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
                    RequestResponseEvent::Message { peer, message } => match message {
                        RequestResponseMessage::Request {
                            request_id: _,
                            request,
                            channel,
                        } => self.inject_request(BitswapChannel::Bitswap(channel), request),
                        RequestResponseMessage::Response {
                            request_id,
                            response,
                        } => self.inject_response(BitswapId::Bitswap(request_id), peer, response),
                    },
                    RequestResponseEvent::ResponseSent { .. } => {}
                    RequestResponseEvent::OutboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        self.inject_outbound_failure(&peer, request_id, &error);
                        #[cfg(feature = "compat")]
                        if let OutboundFailure::UnsupportedProtocols = error {
                            if let Some(id) = self.requests.remove(&BitswapId::Bitswap(request_id))
                            {
                                if let Some(info) = self.query_manager.query_info(id) {
                                    let ty = match info.label {
                                        "have" => RequestType::Have,
                                        "block" => RequestType::Block,
                                        _ => unreachable!(),
                                    };
                                    let request = BitswapRequest { ty, cid: info.cid };
                                    self.requests.insert(BitswapId::Compat(info.cid), id);
                                    tracing::trace!("adding compat peer {}", peer);
                                    self.compat.insert(peer);
                                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                                        peer_id: peer,
                                        handler: NotifyHandler::Any,
                                        event: EitherOutput::Second(CompatMessage::Request(
                                            request,
                                        )),
                                    });
                                }
                            }
                        }
                        if let Some(id) = self.requests.remove(&BitswapId::Bitswap(request_id)) {
                            self.query_manager
                                .inject_response(id, Response::Have(peer, false));
                        }
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
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use blockstore::memory::MemoryDB as Store;
    use futures::prelude::*;
    use libipld::block::Block;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::ipld::Ipld;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use std::sync::Arc;
    use std::time::Duration;

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init()
            .ok();
    }

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

    fn create_block(ipld: Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, &ipld).unwrap()
    }

    struct Peer {
        peer_id: PeerId,
        addr: Multiaddr,
        store: Arc<Store>,
        swarm: Swarm<Bitswap<Store>>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let store = Arc::new(Store::default());
            let mut swarm = Swarm::new(
                trans,
                Bitswap::new(BitswapConfig::new(), store.clone()),
                peer_id,
            );
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                store,
                swarm,
            }
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn store(&mut self) -> Arc<Store> {
            self.store.clone()
        }

        fn swarm(&mut self) -> &mut Swarm<Bitswap<Store>> {
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

        async fn next(&mut self) -> Option<BitswapEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    return Some(event);
                }
            }
        }
    }

    fn assert_progress(event: Option<BitswapEvent>, id: QueryId, missing: usize) {
        if let Some(BitswapEvent::Progress(id2, missing2)) = event {
            assert_eq!(id2, id);
            assert_eq!(missing2, missing);
        } else {
            panic!("{:?} is not a progress event", event);
        }
    }

    fn assert_complete_ok(event: Option<BitswapEvent>, id: QueryId) {
        if let Some(BitswapEvent::Complete(id2, Ok(()))) = event {
            assert_eq!(id2, id);
        } else {
            panic!("{:?} is not a complete event", event);
        }
    }

    #[async_std::test]
    async fn test_bitswap_get() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(&block).unwrap();
        peer1.store().get(block.cid()).unwrap();
        let peer1 = peer1.spawn("peer1");

        let id = peer2
            .swarm()
            .behaviour_mut()
            .get(*block.cid(), std::iter::once(peer1));

        assert_complete_ok(peer2.next().await, id);
    }

    #[async_std::test]
    async fn test_bitswap_cancel_get() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(&block).unwrap();
        let peer1 = peer1.spawn("peer1");

        let id = peer2
            .swarm()
            .behaviour_mut()
            .get(*block.cid(), std::iter::once(peer1));
        peer2.swarm().behaviour_mut().cancel(id);
        let res = peer2.next().now_or_never();
        println!("{:?}", res);
        assert!(res.is_none());
    }

    #[async_std::test]
    async fn test_bitswap_sync() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let b0 = create_block(ipld!({
            "n": 0,
        }));
        let b1 = create_block(ipld!({
            "prev": b0.cid(),
            "n": 1,
        }));
        let b2 = create_block(ipld!({
            "prev": b1.cid(),
            "n": 2,
        }));
        peer1.store().insert(&b0).unwrap();
        peer1.store().insert(&b1).unwrap();
        peer1.store().insert(&b2).unwrap();
        let peer1 = peer1.spawn("peer1");

        let id =
            peer2
                .swarm()
                .behaviour_mut()
                .sync(*b2.cid(), vec![peer1], std::iter::once(*b2.cid()));

        assert_progress(peer2.next().await, id, 1);
        assert_progress(peer2.next().await, id, 1);

        assert_complete_ok(peer2.next().await, id);
    }

    #[async_std::test]
    async fn test_bitswap_cancel_sync() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(&block).unwrap();
        let peer1 = peer1.spawn("peer1");

        let id = peer2.swarm().behaviour_mut().sync(
            *block.cid(),
            vec![peer1],
            std::iter::once(*block.cid()),
        );
        peer2.swarm().behaviour_mut().cancel(id);
        let res = peer2.next().now_or_never();
        println!("{:?}", res);
        assert!(res.is_none());
    }

    #[cfg(feature = "compat")]
    #[async_std::test]
    async fn compat_test() {
        tracing_try_init();
        let cid: Cid = "QmXQsqVRpp2W7fbYZHi4aB2Xkqfd3DpwWskZoLVEYigMKC"
            .parse()
            .unwrap();
        let peer_id: PeerId = "QmRSGx67Kq8w7xSBDia7hQfbfuvauMQGgxcwSWw976x4BS"
            .parse()
            .unwrap();
        let multiaddr: Multiaddr = "/ip4/54.173.33.96/tcp/4001".parse().unwrap();

        let mut peer = Peer::new();
        peer.swarm()
            .behaviour_mut()
            .add_address(&peer_id, multiaddr);
        let id = peer
            .swarm()
            .behaviour_mut()
            .get(cid, std::iter::once(peer_id));
        assert_complete_ok(peer.next().await, id);
    }
}
