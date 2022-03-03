use libipld::cid::multihash::{Code, MultihashDigest};
use libipld::Cid;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_cbor::tags::Tagged;
use std::fmt;

/// Cbor [Cid] codec.
pub const DAG_CBOR: u64 = 0x71;

const MULTIBASE_IDENTITY: u8 = 0;

const CBOR_TAG_CID: u64 = 42;

// Wrapper for correctly serializing the Paych Actor Bytes
#[derive(PartialEq, Eq, Clone, Default, Hash, PartialOrd, Ord, Debug)]
pub struct CidCbor {
    pub bytes: Vec<u8>,
}
impl CidCbor {
    pub fn bytes(&self) -> &Vec<u8> {
        return &self.bytes;
    }
    pub fn to_cid(&self) -> Option<Cid> {
        match Cid::try_from(self.bytes.clone()) {
            Ok(cid) => Some(cid),
            Err(_) => None,
        }
    }
}
impl Serialize for CidCbor {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut cid_bytes = self.bytes().clone();

        // or for all Cid bytes (byte is irrelevant and redundant)
        cid_bytes.insert(0, MULTIBASE_IDENTITY);

        //  add CID tag
        let value = serde_bytes::Bytes::new(&cid_bytes);
        Tagged::new(Some(CBOR_TAG_CID), &value).serialize(s)
    }
}

impl<'de> Deserialize<'de> for CidCbor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = Tagged::<serde_bytes::ByteBuf>::deserialize(deserializer)?;
        match tagged.tag {
            Some(CBOR_TAG_CID) | None => {
                let mut bz = tagged.value.into_vec();

                if bz.first() == Some(&MULTIBASE_IDENTITY) {
                    bz.remove(0);
                }
                return Ok(CidCbor { bytes: bz });
            }
            Some(_) => Err(serde::de::Error::custom("unexpected tag")),
        }
    }
}

impl From<Cid> for CidCbor {
    fn from(cid: Cid) -> Self {
        Self {
            bytes: cid.to_bytes(),
        }
    }
}

impl fmt::Display for CidCbor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(cid) = self.to_cid() {
            write!(f, "{}", cid)
        } else {
            write!(f, "Invalid Cid")
        }
    }
}

/// Constructs a cid with bytes using default version and codec
pub fn new_cid_from_cbor(bz: &[u8], code: Code) -> Cid {
    let hash = code.digest(bz);
    Cid::new_v1(DAG_CBOR, hash)
}

/// Wrapper for serializing and deserializing a Cid from JSON.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CidJson(#[serde(with = "self")] pub Cid);

/// Struct just used as a helper to serialize a cid into a map with key "/"
#[derive(Serialize, Deserialize)]
struct CidMap {
    #[serde(rename = "/")]
    cid: String,
}

/// Wrapper for serializing a cid reference to JSON.
#[derive(Serialize)]
pub struct CidJsonRef<'a>(#[serde(with = "self")] pub &'a Cid);

impl From<CidJson> for Cid {
    fn from(wrapper: CidJson) -> Self {
        wrapper.0
    }
}

pub fn serialize<S>(c: &Cid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    CidMap { cid: c.to_string() }.serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Cid, D::Error>
where
    D: Deserializer<'de>,
{
    let CidMap { cid } = Deserialize::deserialize(deserializer)?;
    cid.parse().map_err(de::Error::custom)
}

//  --------------------------------------------------------------------------------
