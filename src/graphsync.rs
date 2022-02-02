use crate::graphsync_pb as pb;
use crate::network::{
    InboundFailure, OutboundFailure, ProtocolName, ProtocolSupport, RequestId as ReqResId,
    RequestResponse, RequestResponseCodec, RequestResponseConfig, RequestResponseEvent,
    RequestResponseMessage, ResponseChannel,
};
use crate::traversal::{BlockCallbackLoader, Cbor, Error as EncodingError, Progress, Selector};
use async_std::task;
use async_trait::async_trait;
use fnv::FnvHashMap;
use futures::channel::mpsc;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use futures::task::{Context, Poll};
use integer_encoding::{VarIntReader, VarIntWriter};
use libipld::cid::Version;
use libipld::codec::Decode;
use libipld::multihash::{Code, Multihash, MultihashDigest};
use libipld::store::{Store, StoreParams};
use libipld::{Block, Cid, DefaultParams, Ipld, Result as StoreResult};
use libp2p::core::connection::{ConnectionId, ListenerId};
use libp2p::core::{upgrade, ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::{
    DialError, IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    ProtocolsHandler,
};
use protobuf::Message as PBMessage;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::io::{self, Cursor};
use std::marker::PhantomData;
use std::sync::Arc;
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
            max_msg_size: usize::max(P::MAX_BLOCK_SIZE, MAX_CID_SIZE) + 40,
        }
    }
}

impl<P: StoreParams> GraphsyncCodec<P> {
    async fn write_msg<T>(&mut self, io: &mut T, msg: GraphsyncMessage) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        let bytes = msg
            .to_bytes()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        upgrade::write_length_prefixed(io, bytes).await?;
        io.close().await?;
        Ok(())
    }
    async fn read_msg<T>(&mut self, io: &mut T) -> io::Result<GraphsyncMessage>
    where
        T: AsyncRead + Send + Unpin,
    {
        let packet = upgrade::read_length_prefixed(io, self.max_msg_size)
            .await
            .map_err(other)?;
        let message = GraphsyncMessage::from_bytes(&packet)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(message)
    }
}

#[async_trait]
impl<P: StoreParams> RequestResponseCodec for GraphsyncCodec<P> {
    type Protocol = GraphsyncProtocol;
    type Request = GraphsyncMessage;
    type Response = GraphsyncMessage;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Send + Unpin,
    {
        self.read_msg::<T>(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Send + Unpin,
    {
        self.read_msg::<T>(io).await
    }
    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        self.write_msg::<T>(io, req).await
    }
    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        self.write_msg::<T>(io, res).await
    }
}

pub type Channel = ResponseChannel<GraphsyncMessage>;

#[derive(Debug)]
pub enum GraphsyncEvent {
    Progress(RequestId),
    Complete(RequestId, Result<(), EncodingError>),
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
            request_timeout: Duration::from_secs(10),
            connection_keep_alive: Duration::from_secs(10),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Graphsync<S: Store> {
    config: Config,
    inner: RequestResponse<GraphsyncCodec<S::Params>>,
    request_manager: RequestManager,
    response_manager: ResponseManager<S>,
}

impl<S: 'static + Store> Graphsync<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(config: Config, store: S) -> Self {
        let protocols = std::iter::once((GraphsyncProtocol, ProtocolSupport::Full));
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let inner = RequestResponse::new(GraphsyncCodec::default(), protocols, rr_config);
        Self {
            config,
            inner,
            request_manager: Default::default(),
            response_manager: ResponseManager::new(store),
        }
    }
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.inner.add_address(peer_id, addr);
    }
    pub fn request(&mut self, peer_id: PeerId, root: Cid, selector: Selector) -> RequestId {
        self.request_manager.start_request(peer_id, root, selector)
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
}

impl<S: 'static + Store> NetworkBehaviour for Graphsync<S>
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
            while let Some(event) = self.response_manager.next() {
                exit = false;
                match event {
                    ResponseEvent::NewResponse(channel, res) => {
                        self.inner
                            .send_response(channel, GraphsyncMessage::from(res))
                            .ok();
                    }
                }
            }
            while let Some(event) = self.request_manager.next() {
                exit = false;
                match event {
                    RequestEvent::NewRequest(responder, req) => {
                        self.inner
                            .send_request(&responder, GraphsyncMessage::from(req));
                    }
                    RequestEvent::BlockReceived(req_id) => {
                        let event = GraphsyncEvent::Progress(req_id);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                    RequestEvent::Complete(req_id, res) => {
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
                    RequestResponseEvent::Message { peer, message } => match message {
                        RequestResponseMessage::Request {
                            request_id: _,
                            request,
                            channel,
                        } => {
                            println!("received request {:?}", request);
                            for (_, req) in request.requests {
                                println!("received graphsync request");
                                self.response_manager
                                    .inject_request(channel, Request::from(req));
                                break;
                            }
                        }
                        RequestResponseMessage::Response {
                            request_id,
                            response,
                        } => {
                            println!("received response {:?}", response);
                            for (_, res) in response.responses {
                                println!("received graphsync response");
                                self.request_manager
                                    .inject_response(peer, Response::from(res));
                            }
                        }
                    },
                    RequestResponseEvent::ResponseSent { .. } => {}
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
    protocol_id: Cow<'static, [u8]>,
    requests: FnvHashMap<RequestId, GraphsyncRequest>,
    responses: FnvHashMap<RequestId, GraphsyncResponse>,
    blocks: HashMap<Vec<u8>, Vec<u8>>,
}

impl Default for GraphsyncMessage {
    fn default() -> Self {
        Self {
            protocol_id: Cow::Borrowed(b"/ipfs/graphsync/1.0.0"),
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

        let blocks: HashMap<_, _> = msg
            .data
            .into_iter()
            .map(|block| (block.prefix, block.data))
            .collect();

        Ok(GraphsyncMessage {
            protocol_id: Cow::Borrowed(b"/ipfs/graphsync/1.0.0"),
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

#[derive(Debug)]
pub enum RequestEvent {
    NewRequest(PeerId, Request),
    BlockReceived(RequestId),
    Complete(RequestId, Result<(), EncodingError>),
}

#[derive(Default)]
pub struct RequestManager {
    id_counter: i32,
    requests: FnvHashMap<RequestId, Request>,
    events: VecDeque<RequestEvent>,
}

/// RequestManager processes outgoing requests and handles incoming responses
impl RequestManager {
    fn start_request(&mut self, responder: PeerId, root: Cid, selector: Selector) -> RequestId {
        let id = self.id_counter;
        self.id_counter += 1;
        let req = Request { id, root, selector };
        self.requests.insert(id, req.clone());
        self.events
            .push_back(RequestEvent::NewRequest(responder, req.clone()));
        id
    }
    fn inject_response(&mut self, responder: PeerId, response: Response) {
        match response.status {
            ResponseStatusCode::RequestCompletedFull => {
                self.events
                    .push_back(RequestEvent::Complete(response.req_id, Ok(())));
            }
            _ => {}
        }
    }
    pub fn next(&mut self) -> Option<RequestEvent> {
        self.events.pop_front()
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

#[derive(Debug)]
pub enum ResponseEvent {
    NewResponse(Channel, Response),
}

#[derive(Default)]
pub struct ResponseManager<S> {
    store: S,
    events: VecDeque<ResponseEvent>,
}

/// ResponseManager executes requests on the responder side and queues up responses to send back to
/// the requester
impl<S> ResponseManager<S>
where
    S: 'static + Store,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: S) -> Self {
        Self {
            store,
            events: Default::default(),
        }
    }
    pub fn inject_request(&mut self, channel: Channel, request: Request) {
        let cres = Response {
            req_id: request.id,
            status: ResponseStatusCode::RequestCompletedFull,
            extensions: Extensions::default(),
        };
        self.events
            .push_back(ResponseEvent::NewResponse(channel, cres));

        // let node = match self.store.get(&request.root) {
        //     Ok(block) => match block.ipld() {
        //         Ok(node) => node,
        //         Err(e) => {
        //             let res = Response {
        //                 req_id: request.id,
        //                 status: ResponseStatusCode::RequestFailedUnknown,
        //                 extensions: Extensions::default(),
        //             };
        //             return self
        //                 .events
        //                 .push_back(ResponseEvent::NewResponse(channel, res));
        //         }
        //     },
        //     Err(e) => {
        //         let res = Response {
        //             req_id: request.id,
        //             status: ResponseStatusCode::RequestFailedContentNotFound,
        //             extensions: Extensions::default(),
        //         };
        //         return self
        //             .events
        //             .push_back(ResponseEvent::NewResponse(channel, res));
        //     }
        // };
        // task::spawn(async move {
        //     let loader = BlockCallbackLoader::new(self.store, |block| {
        //         let res = Response {
        //             req_id: request.id,
        //             status: ResponseStatusCode::PartialResponse,
        //             extensions: Extensions::default(),
        //         };
        //         self.events
        //             .push_back(ResponseEvent::NewResponse(channel, res));
        //         Ok(())
        //     });
        //     let mut progress = Progress {
        //         loader: Some(loader),
        //         path: Default::default(),
        //         last_block: None,
        //     };
        //     match progress
        //         .walk_adv(&node, request.selector, &|_, _| Ok(()))
        //         .await
        //     {
        //         Ok(()) => {
        //             let res = Response {
        //                 req_id: request.id,
        //                 status: ResponseStatusCode::RequestCompletedFull,
        //                 extensions: Extensions::default(),
        //             };

        //             self.events
        //                 .push_back(ResponseEvent::NewResponse(channel, res));
        //         }
        //         Err(e) => {
        //             let res = Response {
        //                 req_id: request.id,
        //                 status: ResponseStatusCode::RequestFailedUnknown,
        //                 extensions: Extensions::default(),
        //             };
        //             return self
        //                 .events
        //                 .push_back(ResponseEvent::NewResponse(channel, res));
        //         }
        //     };
        // });
    }
    pub fn next(&mut self) -> Option<ResponseEvent> {
        self.events.pop_front()
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
    use super::*;
    use crate::traversal::{RecursionLimit, Selector};
    use futures::prelude::*;
    use hex;
    use libipld::mem::MemStore;
    use libipld::DefaultParams;
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
        let msgEncoded = msg.to_bytes().unwrap();

        let desMsg = GraphsyncMessage::from_bytes(&msgEncoded).unwrap();

        assert_eq!(msgc, desMsg)
    }

    #[test]
    fn proto_msg_compat() {
        // request encoded from JS implementation
        let jsHex = hex::decode("124a0801122401701220340887bdb4718b1632ea61e37ffaab03967013777973ed6bc4b6b5e23e8d7f171a1aa16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0280030003800").unwrap();
        // request encoded from go implementation
        let goHex = hex::decode("12440801122401701220340887bdb4718b1632ea61e37ffaab03967013777973ed6bc4b6b5e23e8d7f171a1aa16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0").unwrap();

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

        let jsDesMsg = GraphsyncMessage::from_bytes(&jsHex).unwrap();

        assert_eq!(msg, jsDesMsg);

        let goDesMsg = GraphsyncMessage::from_bytes(&goHex).unwrap();

        assert_eq!(msg, jsDesMsg)
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

    struct Peer {
        peer_id: PeerId,
        addr: Multiaddr,
        swarm: Swarm<Graphsync<MemStore<DefaultParams>>>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let store = MemStore::<DefaultParams>::default();
            let mut swarm = Swarm::new(trans, Graphsync::new(Config::default(), store), peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
            }
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn swarm(&mut self) -> &mut Swarm<Graphsync<MemStore<DefaultParams>>> {
            &mut self.swarm
        }

        fn spawn(mut self, name: &'static str) -> PeerId {
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

    fn assert_complete_ok(event: Option<GraphsyncEvent>, id: RequestId) {
        if let Some(GraphsyncEvent::Complete(id2, Ok(()))) = event {
            assert_eq!(id2, id);
        } else {
            panic!("{:?} is not a complete event", event);
        }
    }

    #[async_std::test]
    async fn test_request() {
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let peer1 = peer1.spawn("peer1");

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };
        let root =
            Cid::try_from("bafybeibubcd33ndrrmldf2tb4n77vkydszybg53zopwwxrfwwxrd5dl7c4").unwrap();

        let id = peer2.swarm().behaviour_mut().request(peer1, root, selector);

        assert_complete_ok(peer2.next().await, id);
    }
}
