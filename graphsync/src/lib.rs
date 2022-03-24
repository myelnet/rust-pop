mod empty_map;
pub mod res_mgr;
pub mod traversal;

use filecoin::{cid_helpers::CidCbor, types::Cbor};
use futures::io::{AsyncRead, AsyncWrite};
use integer_encoding::{VarIntReader, VarIntWriter};
use libipld::cid::Version;
use libipld::multihash::{Code, Multihash, MultihashDigest};
use libipld::Cid;
use libp2p::core::upgrade;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::collections::BTreeMap;
use std::{io, io::Cursor, vec};
use traversal::{Error as EncodingError, Selector};
use uuid::Uuid;

pub type RequestId = Uuid;

pub type Priority = i32;

fn no_priority(pri: &Priority) -> bool {
    pri == &0
}

#[derive(PartialEq, Clone, Copy, Debug, Serialize, Deserialize)]
pub enum RequestType {
    #[serde(rename = "n")]
    New,
    #[serde(rename = "c")]
    Cancel,
    #[serde(rename = "u")]
    Update,
}

#[derive(PartialEq, Clone, Copy, Eq, Debug, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum ResponseStatusCode {
    RequestAcknowledged = 10,
    PartialResponse = 14,
    RequestPaused = 15,
    RequestCompletedFull = 20,
    RequestCompletedPartial = 21,
    RequestRejected = 30,
    RequestFailedBusy = 31,
    RequestFailedUnknown = 32,
    RequestFailedLegal = 33,
    RequestFailedContentNotFound = 34,
    RequestCancelled = 35,
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

pub type Extensions = BTreeMap<String, ByteBuf>;

// The order of fields on this struct match the ordering of IPLD prime Map node key ordering.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphSyncRequest {
    pub id: RequestId,
    #[serde(rename = "sel")]
    pub selector: Selector,
    pub root: CidCbor,
    #[serde(default, rename = "pri", skip_serializing_if = "no_priority")]
    pub priority: Priority,
    #[serde(rename = "type")]
    pub request_type: RequestType,
    #[serde(default, rename = "ext", skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Extensions>,
}

impl GraphSyncRequest {
    pub fn new(root: Cid, sel: Selector, ext: Option<Extensions>) -> Self {
        Self {
            id: Uuid::new_v4(),
            selector: sel,
            root: root.into(),
            priority: 0,
            request_type: RequestType::New,
            extensions: ext,
        }
    }
    pub fn insert_extension(&mut self, key: &str, val: Vec<u8>) {
        let mut exts = self.extensions.take().unwrap_or_else(|| BTreeMap::new());
        exts.insert(key.to_string(), ByteBuf::from(val));
        self.extensions = Some(exts);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphSyncResponse {
    #[serde(default, rename = "meta", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<GraphSyncLinkData>>,
    #[serde(default, rename = "ext", skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Extensions>,
    #[serde(rename = "stat")]
    pub status: ResponseStatusCode,
    #[serde(rename = "reqid")]
    pub id: RequestId,
}

#[derive(Debug, Clone, Serialize_tuple, Deserialize_tuple, PartialEq)]
pub struct GraphSyncBlock {
    #[serde(with = "serde_bytes")]
    pub prefix: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize_tuple, Deserialize_tuple, PartialEq)]
pub struct GraphSyncLinkData {
    link: CidCbor,
    action: LinkAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LinkAction {
    #[serde(rename = "p")]
    Present,
    #[serde(rename = "d")]
    DuplicateNotSent,
    #[serde(rename = "m")]
    Missing,
    #[serde(rename = "s")]
    DuplicateDAGSkipped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum GraphSyncMessage {
    #[serde(rename = "gs2")]
    V2 {
        #[serde(rename = "req", skip_serializing_if = "Option::is_none")]
        requests: Option<Vec<GraphSyncRequest>>,
        #[serde(rename = "rsp", skip_serializing_if = "Option::is_none")]
        responses: Option<Vec<GraphSyncResponse>>,
        #[serde(rename = "blk", skip_serializing_if = "Option::is_none")]
        blocks: Option<Vec<GraphSyncBlock>>,
    },
}

impl Cbor for GraphSyncMessage {}

impl From<GraphSyncResponse> for GraphSyncMessage {
    fn from(res: GraphSyncResponse) -> Self {
        Self::V2 {
            requests: None,
            responses: Some(vec![res]),
            blocks: None,
        }
    }
}

impl From<GraphSyncRequest> for GraphSyncMessage {
    fn from(req: GraphSyncRequest) -> Self {
        Self::V2 {
            requests: Some(vec![req]),
            responses: None,
            blocks: None,
        }
    }
}

impl GraphSyncMessage {
    pub fn from_blocks(res: GraphSyncResponse, blocks: Vec<GraphSyncBlock>) -> Self {
        Self::V2 {
            requests: None,
            responses: Some(vec![res]),
            blocks: Some(blocks),
        }
    }
    pub async fn from_net<T>(stream: &mut T) -> io::Result<Self>
    where
        T: AsyncRead + Send + Unpin,
    {
        let buf = upgrade::read_length_prefixed(stream, 1024 * 1024).await?;
        if buf.is_empty() {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        let msg = GraphSyncMessage::unmarshal_cbor(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(msg)
    }
    pub async fn to_net<T>(&self, stream: &mut T) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        let buf = self
            .marshal_cbor()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        upgrade::write_length_prefixed(stream, buf).await
    }
    pub fn requests(&self) -> Option<&Vec<GraphSyncRequest>> {
        let Self::V2 { requests, .. } = self;
        requests.as_ref()
    }
    pub fn into_blocks(&self) -> Option<&Vec<GraphSyncBlock>> {
        let Self::V2 { blocks, .. } = self;
        blocks.as_ref()
    }
    pub fn responses(&self) -> Option<&Vec<GraphSyncResponse>> {
        let Self::V2 { responses, .. } = self;
        responses.as_ref()
    }
    pub fn into_inner(
        self,
    ) -> (
        Option<Vec<GraphSyncRequest>>,
        Option<Vec<GraphSyncResponse>>,
        Option<Vec<GraphSyncBlock>>,
    ) {
        let Self::V2 {
            responses,
            requests,
            blocks,
        } = self;
        (requests, responses, blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::traversal::{RecursionLimit, Selector};
    use super::*;
    use futures::io::AsyncRead;
    use hex;
    use std::io::Cursor;
    use std::io::Read;

    #[test]
    fn request_compat() {
        let msg_data = hex::decode("73a163677332a16372657181a462696450326a3f8c261e47ca8b4ea121ba8085676373656ca16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a064726f6f74d82a582500017012209f35d67c4183fdfa4401b5fdc185cf26a0ec18dcf929e3cdb6cdcea09c853a786474797065616e").unwrap();

        let input = msg_data.clone();
        let mut buf = Cursor::new(msg_data);

        let size: usize = buf.read_varint().unwrap();

        let mut bytes = vec![0; size];
        buf.read_exact(&mut bytes).unwrap();
        let msg = GraphSyncMessage::unmarshal_cbor(&bytes).unwrap();

        let mut cbor = msg.marshal_cbor().unwrap();
        let mut out = Vec::new();
        out.write_varint(cbor.len()).unwrap();
        out.append(&mut cbor);
        assert_eq!(out, input);
    }

    #[test]
    fn response_compat() {
        let msg_data = hex::decode("ff02a163677332a16372737082a3646d6574618282d82a582500017112202375ca0c6be62f304dcfa0f801d3f33a066df39cf9dfa11097b0591634ffd02e616d82d82a58250001701220c3c4733ec8affd06cf9e9ff50ffc6bcd2ec85a6170004bb709669c31de94391a6164647374617418226572657169645023f795f246c445588a2a4cb970a525fda463657874a16f486970706974792b486f70706974795864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e35646d6574618282d82a582500017112202375ca0c6be62f304dcfa0f801d3f33a066df39cf9dfa11097b0591634ffd02e617382d82a58250001701220c3c4733ec8affd06cf9e9ff50ffc6bcd2ec85a6170004bb709669c31de94391a617064737461740e657265716964508a63ea3fd29141f48ce56ef043a17c23").unwrap();

        let mut buf = Cursor::new(msg_data);

        let size: usize = buf.read_varint().unwrap();

        let mut bytes = vec![0; size];
        buf.read_exact(&mut bytes).unwrap();
        let msg = GraphSyncMessage::unmarshal_cbor(&bytes).unwrap();

        let mut cbor = msg.marshal_cbor().unwrap();
        let mut out = Vec::new();
        out.write_varint(cbor.len()).unwrap();
        out.append(&mut cbor);
    }

    #[test]
    fn blocks_compat() {
        let msg_data = hex::decode("e301a163677332a163626c6b828244015512205864f55ff8f12508b63ef2bfeca7557ae90df6311a5ec1631b4a1fa843310bd9c3a710eaace5a1bdd72ad0bfe049771c11e756338bd93865e645f1adec9b9c99ef407fbd4fc6859e7904c5ad7dc9bd10a5cc16973d5b28ec1a6dd43d9f82f9f18c3d03418e3582440155122058644204cb9a1e34c5f08e9b20aa76090e70020bb56c0ca3d3af7296cd1058a5112890fed218488f084d8df9e4835fb54ad045ffd936e3bf7261b0426c51352a097816ed74482bb9084b4a7ed8adc517f3371e0e0434b511625cd1a41792243dccdcfe88094b").unwrap();

        let input = msg_data.clone();
        let mut buf = Cursor::new(msg_data);

        let size: usize = buf.read_varint().unwrap();

        let mut bytes = vec![0; size];
        buf.read_exact(&mut bytes).unwrap();
        let msg = GraphSyncMessage::unmarshal_cbor(&bytes).unwrap();

        let mut cbor = msg.marshal_cbor().unwrap();
        let mut out = Vec::new();
        out.write_varint(cbor.len()).unwrap();
        out.append(&mut cbor);
        assert_eq!(out, input);
    }

    #[async_std::test]
    async fn msg_stream() {
        use super::res_mgr::ResponseBuilder;
        use async_std::net::{Shutdown, TcpListener, TcpStream};
        use blockstore::memory::MemoryDB;
        use dag_service::add;
        use futures::prelude::*;
        use libp2p::core::upgrade;
        use rand::prelude::*;
        use std::sync::Arc;

        let store = Arc::new(MemoryDB::default());

        const FILE_SIZE: usize = 1024 * 1024;
        let mut data = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data);

        let root = add(store.clone(), &data).unwrap().unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let server = async move {
            let mut incoming = listener.incoming().into_future().await.0.unwrap().unwrap();
            let msg = GraphSyncMessage::from_net(&mut incoming).await.unwrap();
            let req = msg.requests().unwrap().iter().next().unwrap();
            let mut builder = ResponseBuilder::new(
                req.id,
                req.root.to_cid().unwrap(),
                req.selector.clone(),
                store,
            );
            for msg in builder.by_ref() {
                msg.to_net(&mut incoming).await.unwrap();
            }
        };

        let client = async move {
            let req = GraphSyncRequest::new(root, selector, None);
            let msg = GraphSyncMessage::from(req);
            let mut stream = TcpStream::connect(&listener_addr).await.unwrap();
            msg.to_net(&mut stream).await.unwrap();
            while let Ok(_msg) = GraphSyncMessage::from_net(&mut stream).await {
                println!("received msg");
            }
        };

        futures::join!(server, client);
    }
}
