mod empty_map;
mod graphsync_pb;
mod network;
mod request_manager;
mod response_manager;

pub mod traversal;

use async_trait::async_trait;
use fnv::FnvHashMap;
use futures::io::{AsyncRead, AsyncWrite};
use futures::task::{Context, Poll};
use graphsync_pb as pb;
use network::{
    InboundFailure, MessageCodec, OutboundFailure, ProtocolName, RequestId as ReqResId,
    RequestResponse, RequestResponseConfig, RequestResponseEvent,
};
use request_manager::{RequestEvent, RequestManager};
use response_manager::{ResponseEvent, ResponseManager};
use traversal::{Cbor, Error as EncodingError, Selector};
// use futures::{future::BoxFuture, prelude::*, stream::FuturesUnordered};
use blockstore::types::BlockStore;
use integer_encoding::{VarIntReader, VarIntWriter};
use libipld::cid::Version;
use libipld::codec::Decode;
use libipld::multihash::{Code, Multihash, MultihashDigest};
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use libp2p::core::connection::{ConnectionId, ListenerId};
use libp2p::core::{upgrade, ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::{
    DialError, IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    ProtocolsHandler,
};
use protobuf::Message as PBMessage;
use std::collections::HashMap;
use std::error::Error;
use std::io::{self, Cursor};
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const MAX_CID_SIZE: usize = 4 * 10 + 64;

#[derive(Debug, Clone)]
pub struct GraphsyncProtocol;

impl ProtocolName for GraphsyncProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/ipfs/graphsync/1.0.0"
    }
}

#[derive(Clone)]
pub struct GraphsyncCodec<P> {
    _marker: PhantomData<P>,
    max_msg_size: usize,
}

impl<P: StoreParams> Default for GraphsyncCodec<P> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
            max_msg_size: usize::max(P::MAX_BLOCK_SIZE, MAX_CID_SIZE) + 1000000,
        }
    }
}

#[async_trait]
impl<P: StoreParams> MessageCodec for GraphsyncCodec<P> {
    type Protocol = GraphsyncProtocol;
    type Message = GraphsyncMessage;

    async fn read_message<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Message>
    where
        T: AsyncRead + Send + Unpin,
    {
        let packet = upgrade::read_length_prefixed(io, self.max_msg_size)
            .await
            .map_err(other)?;
        if packet.len() == 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "End of output"));
        }
        let message = GraphsyncMessage::from_bytes(&packet)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(message)
    }
    async fn write_message<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        msg: Self::Message,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        let bytes = msg
            .to_bytes()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        upgrade::write_length_prefixed(io, bytes).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum GraphsyncEvent {
    RequestAccepted(PeerId, GraphsyncRequest),
    ResponseCompleted(PeerId, Vec<RequestId>),
    // We may receive more than one response at once
    ResponseReceived(Vec<GraphsyncResponse>),
    Progress {
        req_id: RequestId,
        link: Cid,
        size: usize,
    },
    Complete(RequestId, Result<(), EncodingError>),
}

pub type IncomingRequestHook =
    Arc<dyn Fn(&PeerId, &GraphsyncRequest) -> (bool, Extensions) + Send + Sync>;

/// requester, root, size
pub type OutgoingBlockHook = Arc<dyn Fn(&PeerId, &Cid, usize) -> Extensions + Send + Sync>;

pub struct GraphsyncHooks {
    incoming_request: IncomingRequestHook,
    outgoing_block: OutgoingBlockHook,
}

impl Default for GraphsyncHooks {
    fn default() -> Self {
        Self {
            incoming_request: Arc::new(|_, _| (true, Extensions::default())),
            outgoing_block: Arc::new(|_, _, _| Extensions::default()),
        }
    }
}

impl GraphsyncHooks {
    pub fn register_incoming_request(&mut self, f: IncomingRequestHook) {
        self.incoming_request = f;
    }
    pub fn register_outgoing_block(&mut self, f: OutgoingBlockHook) {
        self.outgoing_block = f
    }
}

#[derive(Clone)]
pub struct Config {
    /// Timeout of a request.
    pub request_timeout: Duration,
    /// Time a connection is kept alive.
    pub connection_keep_alive: Duration,
}

impl Config {
    pub fn new() -> Self {
        Self {
            request_timeout: Duration::from_secs(10 * 60),
            connection_keep_alive: Duration::from_secs(10 * 60),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Graphsync<S: BlockStore> {
    config: Config,
    inner: RequestResponse<GraphsyncCodec<S::Params>>,
    request_manager: RequestManager<S>,
    response_manager: ResponseManager<S>,
    hooks: Arc<RwLock<GraphsyncHooks>>,
}

impl<S: 'static + BlockStore> Graphsync<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(config: Config, store: Arc<S>) -> Self {
        let protocols = std::iter::once(GraphsyncProtocol);
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let inner = RequestResponse::new(GraphsyncCodec::default(), protocols, rr_config);
        let hooks = Arc::new(RwLock::new(GraphsyncHooks::default()));
        Self {
            config,
            inner,
            request_manager: RequestManager::new(store.clone()),
            response_manager: ResponseManager::new(store.clone(), hooks.clone()),
            hooks,
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
        self.request_manager
            .start_request(peer_id, root, selector, extensions)
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
    pub fn register_incoming_request_hook(&mut self, hook: IncomingRequestHook) {
        self.hooks.write().unwrap().register_incoming_request(hook);
    }
    pub fn register_outgoing_block_hook(&mut self, hook: OutgoingBlockHook) {
        self.hooks.write().unwrap().register_outgoing_block(hook);
    }
}

impl<S: 'static + BlockStore> NetworkBehaviour for Graphsync<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    type ProtocolsHandler =
        <RequestResponse<GraphsyncCodec<S::Params>> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = GraphsyncEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        return self.inner.new_handler();
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
        let mut exit = false;
        while !exit {
            exit = true;
            while let Poll::Ready(Some(event)) = self.response_manager.next(ctx) {
                exit = false;
                match event {
                    ResponseEvent::Accepted(peer, req) => {
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            GraphsyncEvent::RequestAccepted(peer, req),
                        ));
                    }
                    ResponseEvent::Partial(peer, msg) => {
                        self.inner.send_response(&peer, msg);
                    }
                    ResponseEvent::Completed(peer, msg) => {
                        let req_ids: Vec<RequestId> =
                            msg.responses.iter().map(|(_, r)| r.id).collect();
                        self.inner.send_response(&peer, msg);
                        self.inner.finish_outbound(&peer);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            GraphsyncEvent::ResponseCompleted(peer, req_ids),
                        ));
                    }
                }
            }
            while let Poll::Ready(Some(event)) = self.request_manager.next(ctx) {
                exit = false;
                match event {
                    RequestEvent::NewRequest(responder, req) => {
                        self.inner.send_request(&responder, req);
                    }
                    RequestEvent::Progress { req_id, link, size } => {
                        let event = GraphsyncEvent::Progress { req_id, link, size };
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                    RequestEvent::Completed(req_id, res) => {
                        let event = GraphsyncEvent::Complete(req_id, res);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                }
            }
            while let Poll::Ready(event) = self.inner.poll(ctx, pparams) {
                exit = false;
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
                            for (_, req) in &msg.requests {
                                self.response_manager.inject_request(peer, req);
                            }
                        }
                        if !msg.responses.is_empty() {
                            let responses: Vec<GraphsyncResponse> =
                                msg.responses.values().map(|res| res.clone()).collect();
                            self.request_manager.inject_response(msg);
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                                GraphsyncEvent::ResponseReceived(responses),
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
        }
        Poll::Pending
    }
}

pub type ExtensionName = String;
pub type Extensions = HashMap<ExtensionName, Vec<u8>>;

#[derive(Debug, PartialEq, Clone)]
pub struct GraphsyncRequest {
    pub id: RequestId,
    pub root: Cid,
    pub selector: Selector,
    pub extensions: Extensions,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GraphsyncResponse {
    pub id: RequestId,
    pub status: ResponseStatusCode,
    pub extensions: Extensions,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GraphsyncMessage {
    pub requests: FnvHashMap<RequestId, GraphsyncRequest>,
    pub responses: FnvHashMap<RequestId, GraphsyncResponse>,
    pub blocks: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Default for GraphsyncMessage {
    fn default() -> Self {
        Self {
            requests: Default::default(),
            responses: Default::default(),
            blocks: Default::default(),
        }
    }
}

impl GraphsyncMessage {
    pub fn to_bytes(self) -> Result<Vec<u8>, EncodingError> {
        let proto_msg = pb::Message::try_from(self)?;
        let buf: Vec<u8> = proto_msg.write_to_bytes()?;
        Ok(buf)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, EncodingError> {
        let pbmsg = pb::Message::parse_from_bytes(bytes)?;
        Ok(GraphsyncMessage::try_from(pbmsg)?)
    }
}

impl TryFrom<GraphsyncMessage> for pb::Message {
    type Error = EncodingError;
    fn try_from(msg: GraphsyncMessage) -> Result<Self, Self::Error> {
        let mut pb_msg = pb::Message::default();
        for (_, req) in msg.requests {
            pb_msg.requests.push(pb::Message_Request {
                id: req.id,
                root: req.root.to_bytes(),
                selector: req.selector.marshal_cbor()?,
                extensions: req.extensions,
                priority: 0,
                ..Default::default()
            })
        }
        for (_, res) in msg.responses {
            pb_msg.responses.push(pb::Message_Response {
                id: res.id,
                status: res.status.to_i32(),
                extensions: res.extensions,
                ..Default::default()
            })
        }
        for (prefix, data) in msg.blocks {
            pb_msg.data.push(pb::Message_Block {
                data,
                prefix,
                ..Default::default()
            })
        }
        Ok(pb_msg)
    }
}

impl TryFrom<pb::Message> for GraphsyncMessage {
    type Error = EncodingError;
    fn try_from(msg: pb::Message) -> Result<Self, Self::Error> {
        let requests: FnvHashMap<_, _> = msg
            .requests
            .into_iter()
            .map(|r| {
                Ok((
                    r.id,
                    GraphsyncRequest {
                        id: r.id,
                        root: Cid::try_from(r.root)?,
                        selector: Selector::unmarshal_cbor(&r.selector)?,
                        extensions: r.extensions,
                    },
                ))
            })
            .collect::<Result<_, Self::Error>>()?;

        let responses: FnvHashMap<_, _> = msg
            .responses
            .into_iter()
            .map(|r| {
                (
                    r.id,
                    GraphsyncResponse {
                        id: r.id,
                        status: ResponseStatusCode::from_i32(r.status),
                        extensions: r.extensions,
                    },
                )
            })
            .collect();

        let blocks: Vec<(_, _)> = msg
            .data
            .into_iter()
            .map(|block| (block.prefix, block.data))
            .collect();

        Ok(GraphsyncMessage {
            requests,
            responses,
            blocks,
        })
    }
}

impl From<Request> for GraphsyncMessage {
    fn from(req: Request) -> Self {
        let mut msg = Self::default();
        msg.requests.insert(
            req.id,
            GraphsyncRequest {
                id: req.id,
                root: req.root,
                selector: req.selector,
                extensions: Default::default(),
            },
        );
        return msg;
    }
}

impl From<Response> for GraphsyncMessage {
    fn from(res: Response) -> Self {
        let mut msg = Self::default();
        msg.responses.insert(
            res.req_id,
            GraphsyncResponse {
                id: res.req_id,
                status: res.status,
                extensions: res.extensions,
            },
        );
        return msg;
    }
}

pub type RequestId = i32;

#[derive(Debug, Clone)]
pub struct Request {
    id: RequestId,
    root: Cid,
    selector: Selector,
}

impl From<GraphsyncRequest> for Request {
    fn from(req: GraphsyncRequest) -> Self {
        Request {
            id: req.id,
            root: req.root,
            selector: req.selector,
        }
    }
}

#[derive(PartialEq, Clone, Copy, Eq, Debug)]
pub enum ResponseStatusCode {
    RequestAcknowledged,
    PartialResponse,
    RequestPaused,
    RequestCompletedFull,
    RequestCompletedPartial,
    RequestRejected,
    RequestFailedBusy,
    RequestFailedUnknown,
    RequestFailedLegal,
    RequestFailedContentNotFound,
    RequestCancelled,
    Other(i32),
}

impl ResponseStatusCode {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::RequestAcknowledged => 10,
            Self::PartialResponse => 14,
            Self::RequestPaused => 15,
            Self::RequestCompletedFull => 20,
            Self::RequestCompletedPartial => 21,
            Self::RequestRejected => 30,
            Self::RequestFailedBusy => 31,
            Self::RequestFailedUnknown => 32,
            Self::RequestFailedLegal => 33,
            Self::RequestFailedContentNotFound => 34,
            Self::RequestCancelled => 35,
            Self::Other(code) => code,
        }
    }
    pub fn from_i32(code: i32) -> Self {
        match code {
            10 => Self::RequestAcknowledged,
            14 => Self::PartialResponse,
            15 => Self::RequestPaused,
            20 => Self::RequestCompletedFull,
            21 => Self::RequestCompletedPartial,
            30 => Self::RequestRejected,
            31 => Self::RequestFailedBusy,
            32 => Self::RequestFailedUnknown,
            33 => Self::RequestFailedLegal,
            34 => Self::RequestFailedContentNotFound,
            35 => Self::RequestCancelled,
            _ => Self::Other(code),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    req_id: RequestId,
    status: ResponseStatusCode,
    extensions: Extensions,
}

impl From<GraphsyncResponse> for Response {
    fn from(res: GraphsyncResponse) -> Self {
        Response {
            req_id: res.id,
            status: res.status,
            extensions: res.extensions,
        }
    }
}

fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Prefix {
    pub version: Version,
    pub codec: u64,
    pub mh_type: u64,
    pub mh_len: usize,
}

impl Prefix {
    pub fn new_from_bytes(data: &[u8]) -> Result<Prefix, EncodingError> {
        let mut cur = Cursor::new(data);

        let raw_version: u64 = cur.read_varint()?;
        let codec = cur.read_varint()?;
        let mh_type: u64 = cur.read_varint()?;
        let mh_len: usize = cur.read_varint()?;

        let version = Version::try_from(raw_version)?;

        Ok(Prefix {
            version,
            codec,
            mh_type,
            mh_len,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(4);

        res.write_varint(u64::from(self.version)).unwrap();
        res.write_varint(self.codec).unwrap();
        res.write_varint(self.mh_type).unwrap();
        res.write_varint(self.mh_len).unwrap();

        res
    }

    pub fn to_cid(&self, data: &[u8]) -> Result<Cid, EncodingError> {
        let hash: Multihash = Code::try_from(self.mh_type)?.digest(data);
        let cid = Cid::new(self.version, self.codec, hash)?;
        Ok(cid)
    }
}

impl From<Cid> for Prefix {
    fn from(cid: Cid) -> Self {
        Self {
            version: cid.version(),
            codec: cid.codec(),
            mh_type: cid.hash().code(),
            mh_len: cid.hash().size().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::traversal::{RecursionLimit, Selector};
    use super::*;
    use async_std::task;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use futures::prelude::*;
    use hex;
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

    #[test]
    fn proto_msg_roundtrip() {
        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };
        let req = Request {
            id: 1,
            root: Cid::try_from("bafybeibubcd33ndrrmldf2tb4n77vkydszybg53zopwwxrfwwxrd5dl7c4")
                .unwrap(),
            selector,
        };

        let msg = GraphsyncMessage::try_from(req).unwrap();

        let msgc = msg.clone();
        let msg_encoded = msg.to_bytes().unwrap();

        let des_msg = GraphsyncMessage::from_bytes(&msg_encoded).unwrap();

        assert_eq!(msgc, des_msg)
    }

    #[test]
    fn proto_msg_compat() {
        // request encoded from JS implementation
        let js_hex = hex::decode("124a0801122401701220340887bdb4718b1632ea61e37ffaab03967013777973ed6bc4b6b5e23e8d7f171a1aa16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0280030003800").unwrap();
        // request encoded from go implementation
        let go_hex = hex::decode("12440801122401701220340887bdb4718b1632ea61e37ffaab03967013777973ed6bc4b6b5e23e8d7f171a1aa16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0").unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };
        let req = Request {
            id: 1,
            root: Cid::try_from("bafybeibubcd33ndrrmldf2tb4n77vkydszybg53zopwwxrfwwxrd5dl7c4")
                .unwrap(),
            selector,
        };

        let msg = GraphsyncMessage::try_from(req).unwrap();

        let js_des_msg = GraphsyncMessage::from_bytes(&js_hex).unwrap();

        assert_eq!(msg, js_des_msg);

        let _go_des_msg = GraphsyncMessage::from_bytes(&go_hex).unwrap();

        assert_eq!(msg, js_des_msg)
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

    struct Peer<B: 'static + BlockStore>
    where
        Ipld: Decode<<<B>::Params as StoreParams>::Codecs>,
    {
        peer_id: PeerId,
        addr: Multiaddr,
        store: Arc<B>,
        swarm: Swarm<Graphsync<B>>,
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
                Graphsync::new(Config::default(), store.clone()),
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

        fn swarm(&mut self) -> &mut Swarm<Graphsync<B>> {
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
        if let Some(GraphsyncEvent::ResponseReceived(responses)) = event {
            assert_eq!(responses[0].id, id);
        } else {
            panic!("{:?} is not a response event", event);
        }
    }

    fn assert_progress_ok(event: Option<GraphsyncEvent>, id: RequestId, cid: Cid, size2: usize) {
        if let Some(GraphsyncEvent::Progress { req_id, link, size }) = event {
            assert_eq!(req_id, id);
            assert_eq!(link, cid);
            assert_eq!(size, size2);
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
    async fn test_request() {
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

    #[async_std::test]
    async fn test_request_entries() {
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
            limit: RecursionLimit::Depth(1),
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
        assert_complete_ok(peer2.next().await, id);
    }

    #[async_std::test]
    async fn test_unixfs_transfer() {
        use dag_service::{add, cat};
        use rand::prelude::*;

        let peer1 = Peer::new(MemoryBlockStore::default());
        let mut peer2 = Peer::new(MemoryBlockStore::default());
        peer2.add_address(&peer1);

        let store = peer1.store.clone();

        // generate 4MiB of random bytes
        const FILE_SIZE: usize = 4 * 1024 * 1024;
        let mut data = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data);

        let root = add(store.clone(), &data).unwrap();

        let peer1 = peer1.spawn("peer1");

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let cid = root.unwrap();

        let id = peer2
            .swarm()
            .behaviour_mut()
            .request(peer1, cid, selector, Extensions::default());

        let mut n = 0;
        loop {
            if let Some(event) = peer2.next().await {
                match event {
                    GraphsyncEvent::Progress { req_id, size, .. } => {
                        let mut exp_size = 262158;
                        if n == 1 {
                            exp_size = 809;
                        }
                        assert_eq!(req_id, id);
                        assert_eq!(size, exp_size);
                    }
                    GraphsyncEvent::ResponseReceived(responses) => {}
                    GraphsyncEvent::Complete(rid, Ok(())) => {
                        assert_eq!(rid, id);
                        break;
                    }
                    _ => {
                        panic!("transfer failed {:?}", event);
                    }
                }
            } else {
                break;
            }
            n += 1;
        }

        let store = peer2.store.clone();

        let buf = cat(store, cid).unwrap();
        assert_eq!(&buf, &data);
    }
}
