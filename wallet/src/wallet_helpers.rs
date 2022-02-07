use super::errors::Error;
use address::Address;
use crypto::Signature;
use libipld::cid::multihash::{Code, MultihashDigest};
use rand::rngs::OsRng;
use secp256k1::{Message as SecpMessage, PublicKey as SecpPublic, SecretKey as SecpPrivate};

pub fn slice_as_hash(xs: &[u8]) -> &[u8; 32] {
    slice_as_array!(xs, [u8; 32]).expect("bad hash length")
}

/// Cbor [Cid] codec.

/// Return the public key for a given private_key and SignatureType
pub fn to_public(private_key: &[u8]) -> Result<Vec<u8>, Error> {
    let private_key =
        SecpPrivate::parse_slice(private_key).map_err(|err| Error::Other(err.to_string()))?;
    let public_key = SecpPublic::from_secret_key(&private_key);
    Ok(public_key.serialize().to_vec())
}

/// Return a new Address that is of a given SignatureType and uses the supplied public_key
pub fn new_address(public_key: &[u8]) -> Result<Address, Error> {
    let addr = Address::new_secp256k1(public_key).map_err(|err| Error::Other(err.to_string()))?;
    Ok(addr)
}

/// Sign takes in SignatureType, private key and message. Returns a Signature for that message
pub fn sign(private_key: &[u8], msg: &[u8]) -> Result<Signature, Error> {
    let priv_key =
        SecpPrivate::parse_slice(private_key).map_err(|err| Error::Other(err.to_string()))?;
    let msg_hash = Code::Blake2b256.digest(msg);
    let c = slice_as_hash(msg_hash.digest());
    let message = SecpMessage::parse(&c);
    let (sig, recovery_id) = secp256k1::sign(&message, &priv_key);
    let mut new_bytes = [0; 65];
    new_bytes[..64].copy_from_slice(&sig.serialize());
    new_bytes[64] = recovery_id.serialize();
    let crypto_sig = Signature::new_secp256k1(new_bytes.to_vec());
    Ok(crypto_sig)
}

/// Generate a new private key
pub fn generate() -> Result<Vec<u8>, Error> {
    let rng = &mut OsRng::default();

    let key = SecpPrivate::random(rng);
    Ok(key.serialize().to_vec())
}
