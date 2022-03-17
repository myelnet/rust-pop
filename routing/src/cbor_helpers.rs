use libp2p::{Multiaddr, PeerId};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::fmt;

/// Cbor [Cid] codec.
pub const DAG_CBOR: u64 = 0x71;

const MULTIBASE_IDENTITY: u8 = 0;

const CBOR_TAG_CID: u64 = 42;

// Wrapper for correctly serializing the Paych Actor Bytes
#[derive(
    PartialEq, Eq, Clone, Default, Hash, PartialOrd, Ord, Debug, Deserialize_tuple, Serialize_tuple,
)]
pub struct MultiaddrCbor {
    pub bytes: Vec<u8>,
}
impl MultiaddrCbor {
    pub fn bytes(&self) -> &Vec<u8> {
        &self.bytes
    }
    pub fn to_ma(&self) -> Option<Multiaddr> {
        match Multiaddr::try_from(self.bytes.clone()) {
            Ok(ma) => Some(ma),
            Err(_) => None,
        }
    }
}

impl From<Multiaddr> for MultiaddrCbor {
    fn from(ma: Multiaddr) -> Self {
        Self { bytes: ma.to_vec() }
    }
}

impl fmt::Display for MultiaddrCbor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(cid) = self.to_ma() {
            write!(f, "{}", cid)
        } else {
            write!(f, "Invalid Cid")
        }
    }
}

// Wrapper for correctly serializing the Paych Actor Bytes
#[derive(
    PartialEq, Eq, Clone, Default, Hash, PartialOrd, Ord, Debug, Deserialize_tuple, Serialize_tuple,
)]
pub struct PeerIdCbor {
    pub bytes: Vec<u8>,
}
impl PeerIdCbor {
    pub fn bytes(&self) -> &Vec<u8> {
        &self.bytes
    }
    pub fn to_pid(&self) -> Option<PeerId> {
        match PeerId::from_bytes(&self.bytes.clone()) {
            Ok(pid) => Some(pid),
            Err(_) => None,
        }
    }
}

impl From<PeerId> for PeerIdCbor {
    fn from(p: PeerId) -> Self {
        Self {
            bytes: p.to_bytes().to_vec(),
        }
    }
}

impl fmt::Display for PeerIdCbor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(pid) = self.to_pid() {
            write!(f, "{}", pid)
        } else {
            write!(f, "Invalid pid")
        }
    }
}
