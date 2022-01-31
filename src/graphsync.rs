use crate::graphsync_pb as pb;
use crate::traversal::{Cbor, Error as EncodingError, Selector};
use async_trait::async_trait;
use bytes::Bytes;
use fnv::FnvHashMap;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use integer_encoding::{VarIntReader, VarIntWriter};
use libipld::cid::Version;
use libipld::multihash::{Code, Multihash, MultihashDigest};
use libipld::store::StoreParams;
use libipld::Cid;
use libp2p::core::PeerId;
use libp2p::request_response::{ProtocolName, RequestResponseCodec};
use protobuf::Message as PBMessage;
use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::marker::PhantomData;
use unsigned_varint::{aio, codec, io::ReadError};

// version codec hash size (u64 varint is max 10 bytes) + digest
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
    buffer: Vec<u8>,
}

impl<P: StoreParams> Default for GraphsyncCodec<P> {
    fn default() -> Self {
        let capacity = usize::max(P::MAX_BLOCK_SIZE, MAX_CID_SIZE) + 1;
        debug_assert!(capacity <= u32::MAX as usize);
        Self {
            _marker: PhantomData,
            buffer: Vec::with_capacity(capacity),
        }
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
        let msg_len = u32_to_usize(aio::read_u32(&mut *io).await.map_err(|e| match e {
            ReadError::Io(e) => e,
            err => other(err),
        })?);
        self.buffer.resize(msg_len, 0);
        io.read_exact(&mut self.buffer).await?;
        let request = GraphsyncMessage::from_bytes(&self.buffer).map_err(invalid_data)?;
        Ok(request)
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

#[derive(Debug, Clone)]
pub struct GraphsyncResponse {
    pub id: RequestId,
    pub status: ResponseStatusCode,
    pub extensions: Extensions,
}

pub struct GraphsyncMessage {
    requests: FnvHashMap<RequestId, GraphsyncRequest>,
    responses: FnvHashMap<RequestId, GraphsyncResponse>,
    blocks: HashMap<Cid, Vec<u8>>,
    pub(crate) length_codec: codec::UviBytes,
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
            .map(|(cid, data)| pb::Message_Block {
                data,
                prefix: Prefix::from(cid).to_bytes(),
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
            .map(|block| {
                let prefix = Prefix::new_from_bytes(&block.prefix)?;
                let cid = prefix.to_cid(&block.data)?;
                Ok((cid, block.data))
            })
            .collect::<Result<_, Self::Error>>()?;

        let mut length_codec = codec::UviBytes::default();
        length_codec.set_max_len(2048);

        Ok(GraphsyncMessage {
            requests,
            responses,
            blocks,
            length_codec,
        })
    }
}

pub type RequestId = i32;

#[derive(Debug)]
pub struct Request {
    pub id: RequestId,
    pub root: Cid,
    pub selector: Selector,
}

#[derive(Default)]
pub struct RequestManager {
    id_counter: u64,
    requests: FnvHashMap<RequestId, Request>,
}

impl RequestManager {}

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
        }
    }
}

#[cfg(any(target_pointer_width = "64", target_pointer_width = "32"))]
fn u32_to_usize(n: u32) -> usize {
    n as usize
}

fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

fn invalid_data<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
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
