use libp2p::{PeerId};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::fmt;

// Wrapper for correctly serializing the PeerId Bytes
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
