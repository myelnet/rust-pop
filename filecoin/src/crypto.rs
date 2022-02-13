// Copyright 2019-2022 ChainSafe Systems
// SPDX-License-Identifier: Apache-2.0, MIT

use address::{Address, Protocol};
use blake2b_simd::Params;
use libsecp256k1::{recover, Message, RecoveryId, Signature as EcsdaSignature};
use num_derive::FromPrimitive;
use serde::{de, ser};
use serde_repr::*;
use std::borrow::Cow;

/// Secp256k1 signature length in bytes.
pub const SECP_SIG_LEN: usize = 65;
/// Secp256k1 Public key length in bytes.
pub const SECP_PUB_LEN: usize = 65;

pub fn blake2b_256(ingest: &[u8]) -> [u8; 32] {
    let digest = Params::new()
        .hash_length(32)
        .to_state()
        .update(ingest)
        .finalize();

    let mut ret = [0u8; 32];
    ret.clone_from_slice(digest.as_bytes());
    ret
}

pub mod errors {

    use address::Error as AddressError;
    // use encoding::Error as EncodingError;
    use libsecp256k1::Error as SecpError;

    use std::error;
    use thiserror::Error;

    // Copyright 2019-2022 ChainSafe Systems
    // SPDX-License-Identifier: Apache-2.0, MIT

    /// Crypto error
    #[derive(Debug, PartialEq, Error)]
    pub enum Error {
        /// Failed to produce a signature
        #[error("Failed to sign data {0}")]
        SigningError(String),
        /// Unable to perform ecrecover with the given params
        #[error("Could not recover public key from signature: {0}")]
        InvalidRecovery(String),
        /// Provided public key is not understood
        #[error("Invalid generated pub key to create address: {0}")]
        InvalidPubKey(#[from] AddressError),
    }

    impl From<Box<dyn error::Error>> for Error {
        fn from(err: Box<dyn error::Error>) -> Error {
            // Pass error encountered in signer trait as module error type
            Error::SigningError(err.to_string())
        }
    }
    impl From<SecpError> for Error {
        fn from(err: SecpError) -> Error {
            match err {
                SecpError::InvalidRecoveryId => Error::InvalidRecovery(format!("{:?}", err)),
                _ => Error::SigningError(format!("{:?}", err)),
            }
        }
    }
}

/// Signature variants for Filecoin signatures.
#[derive(
    Clone, Debug, PartialEq, FromPrimitive, Copy, Eq, Serialize_repr, Deserialize_repr, Hash,
)]
#[repr(u8)]
pub enum SignatureType {
    Secp256k1 = 1,
}

/// A cryptographic signature, represented in bytes, of any key protocol.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Signature {
    sig_type: SignatureType,
    bytes: Vec<u8>,
}

impl ser::Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut bytes = Vec::with_capacity(self.bytes.len() + 1);
        // Insert signature type byte
        bytes.push(self.sig_type as u8);
        bytes.extend_from_slice(&self.bytes);

        serde_bytes::Serialize::serialize(&bytes, serializer)
    }
}

impl<'de> de::Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let bytes: Cow<'de, [u8]> = serde_bytes::Deserialize::deserialize(deserializer)?;
        if bytes.is_empty() {
            return Err(de::Error::custom("Cannot deserialize empty bytes"));
        }

        // Remove signature type byte
        let sig_type = bytes[0];
        if sig_type != (1u8) {
            return Err(de::Error::custom("Invalid signature type byte (must be 1)"));
        }

        Ok(Signature {
            bytes: bytes[1..].to_vec(),
            sig_type: SignatureType::Secp256k1,
        })
    }
}

impl Signature {
    /// Creates a SECP Signature given the raw bytes.
    pub fn new_secp256k1(bytes: Vec<u8>) -> Self {
        Self {
            sig_type: SignatureType::Secp256k1,
            bytes,
        }
    }

    /// Returns reference to signature bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns [SignatureType] for the signature.
    pub fn signature_type(&self) -> SignatureType {
        self.sig_type
    }

    /// Checks if a signature is valid given data and address.
    pub fn verify(&self, data: &[u8], addr: &Address) -> Result<(), String> {
        match addr.protocol() {
            Protocol::Secp256k1 => verify_secp256k1_sig(self.bytes(), data, addr),
            _ => Err("Address must be resolved to verify a signature".to_owned()),
        }
    }
}

/// Returns `String` error if a secp256k1 signature is invalid.
fn verify_secp256k1_sig(signature: &[u8], data: &[u8], addr: &Address) -> Result<(), String> {
    if signature.len() != SECP_SIG_LEN {
        return Err(format!(
            "Invalid Secp256k1 signature length. Was {}, must be 65",
            signature.len()
        ));
    }

    // blake2b 256 hash
    let hash = blake2b_256(data);

    // Ecrecover with hash and signature
    let mut sig = [0u8; SECP_SIG_LEN];
    sig[..].copy_from_slice(signature);
    let rec_addr = ecrecover(&hash, &sig).map_err(|e| e.to_string())?;

    // check address against recovered address
    if &rec_addr == addr {
        Ok(())
    } else {
        Err("Secp signature verification failed".to_owned())
    }
}

/// Return Address for a message given it's signing bytes hash and signature.
pub fn ecrecover(
    hash: &[u8; 32],
    signature: &[u8; SECP_SIG_LEN],
) -> Result<Address, errors::Error> {
    // generate types to recover key from
    let rec_id = RecoveryId::parse(signature[64])?;
    let message = Message::parse(hash);

    // Signature value without recovery byte
    let mut s = [0u8; 64];
    s.clone_from_slice(signature[..64].as_ref());
    // generate Signature
    let sig = EcsdaSignature::parse_standard(&s)?;

    let key = recover(&message, &sig, &rec_id)?;
    let ret = key.serialize();
    let addr = Address::new_secp256k1(&ret)?;
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use libsecp256k1::{sign, PublicKey, SecretKey};
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn secp_ecrecover() {
        let rng = &mut ChaCha8Rng::seed_from_u64(8);

        let priv_key = SecretKey::random(rng);
        let pub_key = PublicKey::from_secret_key(&priv_key);
        let secp_addr = Address::new_secp256k1(&pub_key.serialize()).unwrap();

        let hash = blake2b_256(&[8, 8]);
        let msg = Message::parse(&hash);

        // Generate signature
        let (sig, recovery_id) = sign(&msg, &priv_key);
        let mut signature = [0; 65];
        signature[..64].copy_from_slice(&sig.serialize());
        signature[64] = recovery_id.serialize();

        assert_eq!(ecrecover(&hash, &signature).unwrap(), secp_addr);
    }
}

// #[cfg(feature = "json")]
pub mod json {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    // Wrapper for serializing and deserializing a Signature from JSON.
    #[derive(Deserialize, Serialize)]
    #[serde(transparent)]
    pub struct SignatureJson(#[serde(with = "self")] pub Signature);

    /// Wrapper for serializing a Signature reference to JSON.
    #[derive(Serialize)]
    #[serde(transparent)]
    pub struct SignatureJsonRef<'a>(#[serde(with = "self")] pub &'a Signature);

    #[derive(Serialize, Deserialize)]
    struct JsonHelper {
        #[serde(rename = "Type")]
        sig_type: SignatureType,
        #[serde(rename = "Data")]
        bytes: String,
    }

    pub fn serialize<S>(m: &Signature, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        JsonHelper {
            sig_type: m.sig_type,
            bytes: base64::encode(&m.bytes),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Signature, D::Error>
    where
        D: Deserializer<'de>,
    {
        let JsonHelper { sig_type, bytes } = Deserialize::deserialize(deserializer)?;
        Ok(Signature {
            sig_type,
            bytes: base64::decode(bytes).map_err(de::Error::custom)?,
        })
    }

    pub mod signature_type {
        use super::*;
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        #[derive(Debug, Deserialize, Serialize)]
        #[serde(rename_all = "lowercase")]
        enum JsonHelperEnum {
            Secp256k1,
        }

        #[derive(Debug, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct SignatureTypeJson(#[serde(with = "self")] pub SignatureType);

        pub fn serialize<S>(m: &SignatureType, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let json = JsonHelperEnum::Secp256k1;
            json.serialize(serializer)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<SignatureType, D::Error>
        where
            D: Deserializer<'de>,
        {
            let json_enum: JsonHelperEnum = Deserialize::deserialize(deserializer)?;

            let signature_type = SignatureType::Secp256k1;
            Ok(signature_type)
        }
    }
}

/// Signer is a trait which allows a key implementation to sign data for an address
pub trait Signer {
    /// Function signs any arbitrary data given the [Address].
    fn sign_bytes(
        &self,
        data: &[u8],
        address: &Address,
    ) -> Result<Signature, Box<dyn std::error::Error>>;
}
