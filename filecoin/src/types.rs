use address::Address;
use cid::{multihash::MultihashDigest, Cid, Code::Blake2b256, Code::Identity, RAW};
use crypto::{signature::Signature, Error as CryptoError, Signer};
use derive_builder::Builder;
use num_bigint::bigint_ser;
use num_bigint::bigint_ser::{BigIntDe, BigIntSer};
use num_bigint::BigInt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_cbor::{error::Error as CborError, from_slice, to_vec};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::error::Error;
use std::fmt;
/// Epoch number of a chain. This acts as a proxy for time within the VM.
pub type ChainEpoch = i64;
use lazy_static::lazy_static;

/// Token type to be used within the VM.
pub type TokenAmount = BigInt;

pub type MethodNum = u64;

/// Serialized bytes to be used as parameters into actor methods.
/// This data is (de)serialized as a byte string.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Hash, Eq, Default)]
#[serde(transparent)]
pub struct Serialized {
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
}

impl Cbor for Serialized {}

impl Serialized {
    /// Constructor if data is encoded already
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Contructor for encoding Cbor encodable structure.
    pub fn serialize<O: Serialize>(obj: O) -> Result<Self, CborError> {
        Ok(Self {
            bytes: to_vec(&obj)?,
        })
    }

    /// Returns serialized bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Deserializes the serialized bytes into a defined type.
    pub fn deserialize<O: DeserializeOwned>(&self) -> Result<O, CborError> {
        Ok(from_slice(&self.bytes)?)
    }
}

pub const PAYCH_ACTOR_CODE_ID_NAME: &[u8] = b"fil/5/paymentchannel";

lazy_static! {
    pub static ref INIT_ACTOR_ADDR: Address = Address::new_id(1);
    pub static ref PAYCH_ACTOR_CODE_ID: Cid =
        Cid::new_v1(RAW, Identity.digest(&PAYCH_ACTOR_CODE_ID_NAME));
}
/// Cbor utility functions for serializable objects
pub trait Cbor: Serialize + DeserializeOwned {
    /// Marshalls cbor encodable object into cbor bytes
    fn marshal_cbor(&self) -> Result<Vec<u8>, CborError> {
        Ok(to_vec(&self)?)
    }

    /// Unmarshals cbor encoded bytes to object
    fn unmarshal_cbor(bz: &[u8]) -> Result<Self, CborError> {
        Ok(from_slice(bz)?)
    }

    /// Returns the content identifier of the raw block of data
    /// Default is Blake2b256 hash
    fn cid(&self) -> Result<Cid, CborError> {
        Ok(cid::new_from_cbor(&self.marshal_cbor()?, Blake2b256))
    }
}

// -----------------------------------------------------------------------------------------

pub enum InitMethod {
    Constructor = 1,
    Exec = 2,
}

pub enum PaychMethod {
    Constructor = 1,
    UpdateChannelState = 2,
    Settle = 3,
    Collect = 4,
}

/// Specifies which `lane`s to be merged with what `nonce` on `channel_update`
#[derive(Default, Clone, Copy, Debug, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct Merge {
    pub lane: usize,
    pub nonce: u64,
}

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct ConstructorParams {
    pub from: Address,
    pub to: Address,
}

impl Cbor for ConstructorParams {}

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct ExecParams {
    pub code_cid: Cid,
    pub constructor_params: Serialized,
}

impl Cbor for ExecParams {}

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct UpdateChannelStateParams {
    pub sv: SignedVoucher,
    #[serde(with = "serde_bytes")]
    pub secret: Vec<u8>,
    // * proof removed in v2
}

impl Cbor for UpdateChannelStateParams {}

/// Modular Verification method
#[derive(Debug, Clone, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct ModVerifyParams {
    pub actor: Address,
    pub method: MethodNum,
    pub data: Serialized,
}

// -----------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct SignedVoucher {
    /// ChannelAddr is the address of the payment channel this signed voucher is valid for
    pub channel_addr: Address,
    /// Min epoch before which the voucher cannot be redeemed
    pub time_lock_min: ChainEpoch,
    /// Max epoch beyond which the voucher cannot be redeemed
    /// set to 0 means no timeout
    pub time_lock_max: ChainEpoch,
    /// (optional) Used by `to` to validate
    #[serde(with = "serde_bytes")]
    pub secret_pre_image: Vec<u8>,
    /// (optional) Specified by `from` to add a verification method to the voucher
    pub extra: Option<ModVerifyParams>,
    /// Specifies which lane the Voucher merges into (will be created if does not exist)
    pub lane: usize,
    /// Set by `from` to prevent redemption of stale vouchers on a lane
    pub nonce: u64,
    /// Amount voucher can be redeemed for
    #[serde(with = "bigint_ser")]
    pub amount: BigInt,
    /// (optional) Can extend channel min_settle_height if needed
    pub min_settle_height: ChainEpoch,

    /// (optional) Set of lanes to be merged into `lane`
    pub merges: Vec<Merge>,

    /// Sender's signature over the voucher (sign on none)
    pub signature: Option<Signature>,
}

// -----------------------------------------------------------------------------------------

/// Message interface to interact with Signed and unsigned messages in a generic context.
pub trait Message {
    /// Returns the from address of the message.
    fn from(&self) -> &Address;
    /// Returns the destination address of the message.
    fn to(&self) -> &Address;
    /// Returns the message sequence or nonce.
    fn nonce(&self) -> u64;
    /// Returns the amount sent in message.
    fn value(&self) -> &TokenAmount;
    /// Returns the method number to be called.
    fn method(&self) -> MethodNum;
    /// Returns the encoded parameters for the method call.
    fn params(&self) -> &Serialized;
    /// sets the gas limit for the message.
    fn set_gas_limit(&mut self, amount: i64);
    /// sets a new sequence to the message.
    fn set_sequence(&mut self, sequence: u64);
    /// Returns the gas limit for the message.
    fn gas_limit(&self) -> i64;
    /// Returns the required funds for the message.
    fn required_funds(&self) -> TokenAmount;
    /// gets gas fee cap for the message.
    fn gas_fee_cap(&self) -> &TokenAmount;
    /// gets gas premium for the message.
    fn gas_premium(&self) -> &TokenAmount;
    /// sets the gas fee cap.
    fn set_gas_fee_cap(&mut self, cap: TokenAmount);
    /// sets the gas premium.
    fn set_gas_premium(&mut self, prem: TokenAmount);
}

// -----------------------------------------------------------------------------------------

#[derive(PartialEq, Clone, Debug, Builder, Hash, Eq)]
#[builder(name = "MessageBuilder")]
pub struct UnsignedMessage {
    #[builder(default)]
    pub version: i64,
    pub from: Address,
    pub to: Address,
    #[builder(default)]
    pub nonce: u64,
    #[builder(default)]
    pub value: TokenAmount,
    #[builder(default)]
    pub method: MethodNum,
    #[builder(default)]
    pub params: Serialized,
    #[builder(default)]
    pub gas_limit: i64,
    #[builder(default)]
    pub gas_fee_cap: TokenAmount,
    #[builder(default)]
    pub gas_premium: TokenAmount,
}

impl UnsignedMessage {
    pub fn builder() -> MessageBuilder {
        MessageBuilder::default()
    }

    /// Helper function to convert the message into signing bytes.
    /// This function returns the message `Cid` bytes.
    pub fn to_signing_bytes(&self) -> Vec<u8> {
        // Safe to unwrap here, unsigned message cannot fail to serialize.
        self.cid().unwrap().to_bytes()
    }

    // /// Helper function to convert the message into signing bytes.
    // /// This function returns the message `Cid` bytes.
    // pub fn to_signing_bytes(&self) -> Vec<u8> {
    //     // Safe to unwrap here, unsigned message cannot fail to serialize.
    //     self.cid().unwrap().to_bytes()
    // }

    /// Semantic validation and validates the message has enough gas.
    #[cfg(feature = "proofs")]
    pub fn valid_for_block_inclusion(
        &self,
        min_gas: i64,
        version: fil_types::NetworkVersion,
    ) -> Result<(), String> {
        use fil_types::{NetworkVersion, BLOCK_GAS_LIMIT, TOTAL_FILECOIN, ZERO_ADDRESS};
        use num_traits::Signed;
        if self.version != 0 {
            return Err(format!("Message version: {} not  supported", self.version));
        }
        if self.to == *ZERO_ADDRESS && version >= NetworkVersion::V7 {
            return Err("invalid 'to' address".to_string());
        }
        if self.value.is_negative() {
            return Err("message value cannot be negative".to_string());
        }
        if self.value > *TOTAL_FILECOIN {
            return Err("message value cannot be greater than total FIL supply".to_string());
        }
        if self.gas_fee_cap.is_negative() {
            return Err("gas_fee_cap cannot be negative".to_string());
        }
        if self.gas_premium.is_negative() {
            return Err("gas_premium cannot be negative".to_string());
        }
        if self.gas_premium > self.gas_fee_cap {
            return Err("gas_fee_cap less than gas_premium".to_string());
        }
        if self.gas_limit > BLOCK_GAS_LIMIT {
            return Err("gas_limit cannot be greater than block gas limit".to_string());
        }

        if self.gas_limit < min_gas {
            return Err(format!(
                "gas_limit {} cannot be less than cost {} of storing a message on chain",
                self.gas_limit, min_gas
            ));
        }

        Ok(())
    }
}

impl Serialize for UnsignedMessage {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (
            &self.version,
            &self.to,
            &self.from,
            &self.nonce,
            BigIntSer(&self.value),
            &self.gas_limit,
            BigIntSer(&self.gas_fee_cap),
            BigIntSer(&self.gas_premium),
            &self.method,
            &self.params,
        )
            .serialize(s)
    }
}

impl<'de> Deserialize<'de> for UnsignedMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (
            version,
            to,
            from,
            nonce,
            BigIntDe(value),
            gas_limit,
            BigIntDe(gas_fee_cap),
            BigIntDe(gas_premium),
            method,
            params,
        ) = Deserialize::deserialize(deserializer)?;
        Ok(Self {
            version,
            from,
            to,
            nonce,
            value,
            method,
            params,
            gas_limit,
            gas_fee_cap,
            gas_premium,
        })
    }
}

impl Message for UnsignedMessage {
    fn from(&self) -> &Address {
        &self.from
    }
    fn to(&self) -> &Address {
        &self.to
    }
    fn nonce(&self) -> u64 {
        self.nonce
    }
    fn value(&self) -> &TokenAmount {
        &self.value
    }
    fn method(&self) -> MethodNum {
        self.method
    }
    fn params(&self) -> &Serialized {
        &self.params
    }
    fn set_sequence(&mut self, new_sequence: u64) {
        self.nonce = new_sequence
    }
    fn gas_limit(&self) -> i64 {
        self.gas_limit
    }
    fn gas_fee_cap(&self) -> &TokenAmount {
        &self.gas_fee_cap
    }
    fn gas_premium(&self) -> &TokenAmount {
        &self.gas_premium
    }
    fn set_gas_limit(&mut self, token_amount: i64) {
        self.gas_limit = token_amount
    }
    fn set_gas_fee_cap(&mut self, cap: TokenAmount) {
        self.gas_fee_cap = cap;
    }
    fn set_gas_premium(&mut self, prem: TokenAmount) {
        self.gas_premium = prem;
    }
    fn required_funds(&self) -> TokenAmount {
        let total: TokenAmount = self.gas_fee_cap() * self.gas_limit();
        total + self.value()
    }
}

impl Cbor for UnsignedMessage {}

pub mod unsigned_json {
    use super::*;
    use address::json::AddressJson;
    use cid::Cid;
    use num_bigint::bigint_ser;
    use serde::ser;

    /// Wrapper for serializing and deserializing a UnsignedMessage from JSON.
    #[derive(Deserialize, Serialize, Debug)]
    #[serde(transparent)]
    pub struct UnsignedMessageJson(#[serde(with = "self")] pub UnsignedMessage);

    /// Wrapper for serializing a UnsignedMessage reference to JSON.
    #[derive(Serialize)]
    #[serde(transparent)]
    pub struct UnsignedMessageJsonRef<'a>(#[serde(with = "self")] pub &'a UnsignedMessage);

    impl From<UnsignedMessageJson> for UnsignedMessage {
        fn from(wrapper: UnsignedMessageJson) -> Self {
            wrapper.0
        }
    }

    impl From<UnsignedMessage> for UnsignedMessageJson {
        fn from(wrapper: UnsignedMessage) -> Self {
            UnsignedMessageJson(wrapper)
        }
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct JsonHelper {
        version: i64,
        to: AddressJson,
        from: AddressJson,
        #[serde(rename = "Nonce")]
        nonce: u64,
        #[serde(with = "bigint_ser::json")]
        value: TokenAmount,
        gas_limit: i64,
        #[serde(with = "bigint_ser::json")]
        gas_fee_cap: TokenAmount,
        #[serde(with = "bigint_ser::json")]
        gas_premium: TokenAmount,
        #[serde(rename = "Method")]
        method: u64,
        params: Option<String>,
        #[serde(default, rename = "CID", with = "cid::json::opt")]
        cid: Option<Cid>,
    }

    pub fn serialize<S>(m: &UnsignedMessage, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        JsonHelper {
            version: m.version,
            to: m.to.into(),
            from: m.from.into(),
            nonce: m.nonce,
            value: m.value.clone(),
            gas_limit: m.gas_limit,
            gas_fee_cap: m.gas_fee_cap.clone(),
            gas_premium: m.gas_premium.clone(),
            method: m.method,
            params: Some(base64::encode(&m.params.bytes())),
            cid: Some(m.cid().map_err(ser::Error::custom)?),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<UnsignedMessage, D::Error>
    where
        D: Deserializer<'de>,
    {
        let m: JsonHelper = Deserialize::deserialize(deserializer)?;
        Ok(UnsignedMessage {
            version: m.version,
            to: m.to.into(),
            from: m.from.into(),
            nonce: m.nonce,
            value: m.value,
            gas_limit: m.gas_limit,
            gas_fee_cap: m.gas_fee_cap,
            gas_premium: m.gas_premium,
            method: m.method,
            params: Serialized::new(
                base64::decode(&m.params.unwrap_or_else(|| "".to_string())).unwrap(),
            ), // .map_err(de::Error::custom)?,
        })
    }
}

// -----------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct MessageError {
    details: String,
}

impl MessageError {
    pub fn new(msg: &str) -> MessageError {
        MessageError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for MessageError {
    fn description(&self) -> &str {
        &self.details
    }
}

// -----------------------------------------------------------------------------------------

/// Represents a wrapped message with signature bytes.
#[derive(PartialEq, Clone, Debug, Serialize_tuple, Deserialize_tuple, Hash, Eq)]
pub struct SignedMessage {
    pub message: UnsignedMessage,
    pub signature: Signature,
}

impl SignedMessage {
    /// Generate new signed message from an unsigned message and a signer.
    pub fn new<S: Signer>(message: UnsignedMessage, signer: &S) -> Result<Self, CryptoError> {
        let bz = message.to_signing_bytes();

        let signature = signer.sign_bytes(&bz, message.from())?;

        Ok(SignedMessage { message, signature })
    }

    /// Generate a new signed message from fields.
    pub fn new_from_parts(
        message: UnsignedMessage,
        signature: Signature,
    ) -> Result<SignedMessage, String> {
        signature.verify(&message.to_signing_bytes(), message.from())?;
        Ok(SignedMessage { message, signature })
    }

    /// Returns reference to the unsigned message.
    pub fn message(&self) -> &UnsignedMessage {
        &self.message
    }

    /// Returns signature of the signed message.
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Consumes self and returns it's unsigned message.
    pub fn into_message(self) -> UnsignedMessage {
        self.message
    }

    /// Verifies that the from address of the message generated the signature.
    pub fn verify(&self) -> Result<(), String> {
        self.signature
            .verify(&self.message.to_signing_bytes(), self.from())
    }
}

impl Message for SignedMessage {
    fn from(&self) -> &Address {
        self.message.from()
    }
    fn to(&self) -> &Address {
        self.message.to()
    }
    fn nonce(&self) -> u64 {
        self.message.nonce()
    }
    fn value(&self) -> &TokenAmount {
        self.message.value()
    }
    fn method(&self) -> MethodNum {
        self.message.method()
    }
    fn params(&self) -> &Serialized {
        self.message.params()
    }
    fn gas_limit(&self) -> i64 {
        self.message.gas_limit()
    }
    fn set_gas_limit(&mut self, token_amount: i64) {
        self.message.set_gas_limit(token_amount)
    }
    fn set_sequence(&mut self, new_sequence: u64) {
        self.message.set_sequence(new_sequence)
    }
    fn required_funds(&self) -> TokenAmount {
        self.message.required_funds()
    }
    fn gas_fee_cap(&self) -> &TokenAmount {
        self.message.gas_fee_cap()
    }
    fn gas_premium(&self) -> &TokenAmount {
        self.message.gas_premium()
    }

    fn set_gas_fee_cap(&mut self, cap: TokenAmount) {
        self.message.set_gas_fee_cap(cap);
    }

    fn set_gas_premium(&mut self, prem: TokenAmount) {
        self.message.set_gas_premium(prem);
    }
}

impl Cbor for SignedMessage {}

// #[cfg(feature = "json")]
pub mod signed_json {
    use super::*;
    use cid::Cid;
    use crypto::signature;
    use serde::{ser, Deserialize, Deserializer, Serialize, Serializer};

    /// Wrapper for serializing and deserializing a SignedMessage from JSON.
    #[derive(Debug, Deserialize, Serialize)]
    #[serde(transparent)]
    pub struct SignedMessageJson(#[serde(with = "self")] pub SignedMessage);

    /// Wrapper for serializing a SignedMessage reference to JSON.
    #[derive(Serialize)]
    #[serde(transparent)]
    pub struct SignedMessageJsonRef<'a>(#[serde(with = "self")] pub &'a SignedMessage);

    impl From<SignedMessageJson> for SignedMessage {
        fn from(wrapper: SignedMessageJson) -> Self {
            wrapper.0
        }
    }

    impl From<SignedMessage> for SignedMessageJson {
        fn from(msg: SignedMessage) -> Self {
            SignedMessageJson(msg)
        }
    }

    pub fn serialize<S>(m: &SignedMessage, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct SignedMessageSer<'a> {
            #[serde(with = "unsigned_json")]
            message: &'a UnsignedMessage,
            #[serde(with = "signature::json")]
            signature: &'a Signature,
            #[serde(default, rename = "CID", with = "cid::json::opt")]
            cid: Option<Cid>,
        }
        SignedMessageSer {
            message: &m.message,
            signature: &m.signature,
            cid: Some(m.cid().map_err(ser::Error::custom)?),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SignedMessage, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Serialize, Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct SignedMessageDe {
            #[serde(with = "unsigned_json")]
            message: UnsignedMessage,
            #[serde(with = "signature::json")]
            signature: Signature,
        }
        let SignedMessageDe { message, signature } = Deserialize::deserialize(deserializer)?;
        Ok(SignedMessage { message, signature })
    }
}
