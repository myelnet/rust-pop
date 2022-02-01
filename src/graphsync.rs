use crate::graphsync_pb as pb;
use crate::traversal::{Cbor, Error as EncodingError, Selector};
use fnv::FnvHashMap;
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use futures::task::{Context, Poll};
use integer_encoding::{VarIntReader, VarIntWriter};
use libipld::cid::Version;
use libipld::multihash::{Code, Multihash, MultihashDigest};
use libipld::Cid;
use libp2p::core::connection::ConnectionId;
use libp2p::core::{upgrade, InboundUpgrade, OutboundUpgrade, PeerId, UpgradeInfo};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, OneShotHandler, PollParameters,
};
use protobuf::Message as PBMessage;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Cursor};
use std::iter;

const MAX_BUF_SIZE: usize = 524_288;

#[derive(Debug, Clone)]
pub struct GraphsyncProtocol {
    protocol_id: Cow<'static, [u8]>,
    max_transmit_size: usize,
}

impl Default for GraphsyncProtocol {
    fn default() -> Self {
        Self {
            protocol_id: Cow::Borrowed(b"/ipfs/graphsync/1.0.0"),
            max_transmit_size: MAX_BUF_SIZE,
        }
    }
}

impl GraphsyncProtocol {
    pub fn new(id: impl Into<Cow<'static, [u8]>>, max_transmit_size: usize) -> Self {
        Self {
            protocol_id: id.into(),
            max_transmit_size,
        }
    }
}

impl UpgradeInfo for GraphsyncProtocol {
    type Info = Cow<'static, [u8]>;
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(self.protocol_id.clone())
    }
}

impl<TSocket> InboundUpgrade<TSocket> for GraphsyncProtocol
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = InboundMessage;
    type Error = EncodingError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let packet = upgrade::read_length_prefixed(&mut socket, self.max_transmit_size)
                .await
                .map_err(other)?;
            socket.close().await?;
            let message = GraphsyncMessage::from_bytes(&packet)?;
            Ok(InboundMessage(message))
        })
    }
}

#[derive(Debug)]
pub struct InboundMessage(GraphsyncMessage);

impl From<()> for InboundMessage {
    fn from(_: ()) -> Self {
        Self(Default::default())
    }
}

#[derive(Debug)]
pub enum GraphsyncEvent {
    Progress(RequestId),
    Complete(RequestId, Result<(), EncodingError>),
}

#[derive(Clone)]
pub struct Config {
    pub protocol_id: Cow<'static, [u8]>,
}

impl Config {
    pub fn new(id: impl Into<Cow<'static, [u8]>>) -> Self {
        Self {
            protocol_id: id.into(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            protocol_id: Cow::Borrowed(b"/ipfs/graphsync/1.0.0"),
        }
    }
}

pub struct Graphsync {
    config: Config,
    request_manager: RequestManager,
}

impl Graphsync {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            request_manager: Default::default(),
        }
    }
    pub fn request(&mut self, peer_id: PeerId, root: Cid, selector: Selector) -> RequestId {
        self.request_manager.start_request(peer_id, root, selector)
    }
}

impl Default for Graphsync {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl NetworkBehaviour for Graphsync {
    type ProtocolsHandler = OneShotHandler<GraphsyncProtocol, GraphsyncMessage, InboundMessage>;
    type OutEvent = GraphsyncEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        OneShotHandler::default()
    }

    fn inject_event(&mut self, peer_id: PeerId, conn: ConnectionId, event: InboundMessage) {}
    fn poll(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        let mut exit = false;
        while !exit {
            exit = true;
            while let Some(event) = self.request_manager.next() {
                exit = false;
                match event {
                    RequestEvent::NewRequest(req) => {
                        return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                            peer_id: req.responder,
                            handler: NotifyHandler::Any,
                            event: GraphsyncMessage::from(req),
                        });
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

impl UpgradeInfo for GraphsyncMessage {
    type Info = Cow<'static, [u8]>;
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(self.protocol_id.clone())
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for GraphsyncMessage
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = EncodingError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let bytes = self.to_bytes()?;
            upgrade::write_length_prefixed(&mut socket, bytes).await?;
            socket.close().await?;
            Ok(())
        })
    }
}

impl TryFrom<GraphsyncMessage> for pb::Message {
    type Error = EncodingError;
    fn try_from(msg: GraphsyncMessage) -> Result<Self, Self::Error> {
        let requests: protobuf::RepeatedField<_> = msg
            .requests
            .into_iter()
            .map(|(_, req)| {
                Ok(pb::Message_Request {
                    id: req.id,
                    root: req.root.to_bytes(),
                    selector: req.selector.marshal_cbor()?,
                    extensions: req.extensions,
                    priority: 0,
                    ..Default::default()
                })
            })
            .collect::<Result<_, Self::Error>>()?;

        let responses: protobuf::RepeatedField<_> = msg
            .responses
            .into_iter()
            .map(|(_, res)| pb::Message_Response {
                id: res.id,
                status: res.status.to_i32(),
                extensions: res.extensions,
                ..Default::default()
            })
            .collect();

        let data: protobuf::RepeatedField<_> = msg
            .blocks
            .into_iter()
            .map(|(prefix, data)| pb::Message_Block {
                data,
                prefix,
                ..Default::default()
            })
            .collect();

        Ok(pb::Message {
            requests,
            responses,
            data,
            ..Default::default()
        })
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

pub type RequestId = i32;

#[derive(Debug, Clone)]
pub struct Request {
    id: RequestId,
    root: Cid,
    selector: Selector,
    responder: PeerId,
}

#[derive(Debug)]
pub enum RequestEvent {
    NewRequest(Request),
    BlockReceived(RequestId),
    Complete(RequestId, Result<(), EncodingError>),
}

#[derive(Default)]
pub struct RequestManager {
    id_counter: i32,
    requests: FnvHashMap<RequestId, Request>,
    events: VecDeque<RequestEvent>,
}

impl RequestManager {
    fn start_request(&mut self, responder: PeerId, root: Cid, selector: Selector) -> RequestId {
        let id = self.id_counter;
        self.id_counter += 1;
        let req = Request {
            id,
            root,
            selector,
            responder,
        };
        self.requests.insert(id, req.clone());
        self.events.push_back(RequestEvent::NewRequest(req.clone()));
        id
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
    use hex;
    use std::str::FromStr;

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
            responder: PeerId::from_str("12D3KooWHFrmLWTTDD4NodngtRMEVYgxrsDMp4F9iSwYntZ9WjHa")
                .unwrap(),
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
            responder: PeerId::from_str("12D3KooWHFrmLWTTDD4NodngtRMEVYgxrsDMp4F9iSwYntZ9WjHa")
                .unwrap(),
        };

        let msg = GraphsyncMessage::try_from(req).unwrap();

        let jsDesMsg = GraphsyncMessage::from_bytes(&jsHex).unwrap();

        assert_eq!(msg, jsDesMsg);

        let goDesMsg = GraphsyncMessage::from_bytes(&goHex).unwrap();

        assert_eq!(msg, jsDesMsg)
    }
}
