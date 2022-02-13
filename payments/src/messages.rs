use address::Address;
use filecoin::types::*;
use std::error::Error;
use std::fmt;
use std::vec::Vec;

// Printable error that is thrown when constructing a payment message
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

// Messages can create payment channels, update them, settle them or collect funds.
pub trait MessageBuilder {
    fn create(&self, to: Address, amount: TokenAmount) -> Result<UnsignedMessage, MessageError>;
    fn update(
        &self,
        paych: Address,
        voucher: SignedVoucher,
        secret: Vec<u8>,
    ) -> Result<UnsignedMessage, MessageError>;
    fn settle(&self, paych: Address) -> Result<UnsignedMessage, MessageError>;
    fn collect(&self, paych: Address) -> Result<UnsignedMessage, MessageError>;
}

#[derive(Debug)]
pub struct PaymentMessage {
    pub from: Address,
}

impl MessageBuilder for PaymentMessage {
    fn create(&self, to: Address, amount: TokenAmount) -> Result<UnsignedMessage, MessageError> {
        // Use the builder pattern to generate a message
        let params = Serialized::new(
            ConstructorParams {
                from: self.from,
                to: to,
            }
            .marshal_cbor()
            .unwrap(),
        );

        let exec = Serialized::new(
            ExecParams {
                code_cid: PAYCH_ACTOR_CODE_ID.clone(),
                constructor_params: params,
            }
            .marshal_cbor()
            .unwrap(),
        );

        let _message = match UnsignedMessage::builder()
            .to(*INIT_ACTOR_ADDR)
            .from(self.from)
            .nonce(1) // optional
            .value(amount) // optional
            .method(InitMethod::Exec as u64) // optional
            .params(exec) // optional
            // .gas_limit(0) // optional
            // .version(0) // optional
            .build()
        {
            Ok(message) => return Ok(message),
            Err(e) => return Err(MessageError::new(&e)),
        };
    }
    fn update(
        &self,
        paych: Address,
        voucher: SignedVoucher,
        secret: Vec<u8>,
    ) -> Result<UnsignedMessage, MessageError> {
        let params = Cbor::marshal_cbor(&UpdateChannelStateParams {
            sv: voucher,
            secret: secret,
        });

        match UnsignedMessage::builder()
            .to(paych)
            .from(self.from)
            // .nonce(0) // optional
            .value(TokenAmount::from(0u8)) // optional
            .method(PaychMethod::UpdateChannelState as u64) // optional
            .params(Serialized::new(params.unwrap())) // optional
            // .gas_limit(0) // optional
            // .version(0) // optional
            .build()
        {
            Ok(message) => return Ok(message),
            Err(e) => return Err(MessageError::new(&e)),
        };
    }
    fn settle(&self, paych: Address) -> Result<UnsignedMessage, MessageError> {
        match UnsignedMessage::builder()
            .to(paych)
            .from(self.from)
            // .sequence(0) // optional
            .value(TokenAmount::from(0u8)) // optional
            .method(PaychMethod::Settle as u64) // optional
            // .gas_limit(0) // optional
            // .version(0) // optional
            .build()
        {
            Ok(message) => return Ok(message),
            Err(e) => return Err(MessageError::new(&e)),
        };
    }
    fn collect(&self, paych: Address) -> Result<UnsignedMessage, MessageError> {
        match UnsignedMessage::builder()
            .to(paych)
            .from(self.from)
            // .sequence(0) // optional
            .value(TokenAmount::from(0u8)) // optional
            .method(PaychMethod::Collect as u64) // optional
            // .gas_limit(0) // optional
            // .version(0) // optional
            .build()
        {
            Ok(message) => return Ok(message),
            Err(e) => return Err(MessageError::new(&e)),
        };
    }
}

#[cfg(test)]
mod tests {

    #[test]
    pub fn test_messages() {
        use super::*;

        let key_store = wallet::KeyStore::new(wallet::KeyStoreConfig::Memory).unwrap();
        let mut w = wallet::Wallet::new(key_store);

        let new_priv_key = base64::decode("TaFDsadBYd34tSWmOejqj8VCrO9odoywML5u834aJ1E=").unwrap();

        let key_info =
            wallet::KeyInfo::new(filecoin::crypto::SignatureType::Secp256k1, new_priv_key);

        let address = w.import(key_info).unwrap();

        let new_priv_key_2 =
            base64::decode("XLUdUamDdjHc34jTjZi0gnpCebstzdzeS2tVevUiw/Q=").unwrap();

        let key_info_2 =
            wallet::KeyInfo::new(filecoin::crypto::SignatureType::Secp256k1, new_priv_key_2);

        let address_2 = w.import(key_info_2).unwrap();

        let msg = PaymentMessage { from: address };

        let create_channel = msg
            .create(address_2, filecoin::types::TokenAmount::from(123u8))
            .unwrap();

        let sig = w
            .sign(&address, &create_channel.to_signing_bytes())
            .unwrap();

        let true_sig = Vec::from([
            128, 15, 61, 194, 120, 95, 239, 49, 49, 199, 161, 169, 116, 171, 72, 120, 38, 33, 12,
            230, 235, 168, 208, 68, 48, 90, 144, 36, 219, 185, 119, 36, 92, 255, 27, 159, 32, 188,
            238, 211, 54, 168, 78, 204, 88, 176, 114, 239, 7, 161, 110, 9, 226, 129, 157, 130, 166,
            136, 79, 163, 157, 94, 15, 180, 0,
        ]);

        assert_eq!(*sig.bytes(), true_sig);

        let paych_addr = Address::new_id(101);

        let settle_channel = msg.settle(paych_addr).unwrap();

        let sig = w
            .sign(&address, &settle_channel.to_signing_bytes())
            .unwrap();

        let true_sig = Vec::from([
            14, 156, 97, 45, 94, 89, 250, 186, 13, 45, 24, 62, 79, 53, 51, 225, 253, 215, 94, 140,
            204, 194, 24, 98, 228, 228, 77, 62, 120, 39, 139, 69, 89, 200, 47, 232, 88, 67, 192,
            231, 225, 228, 62, 155, 196, 186, 123, 117, 25, 109, 76, 98, 239, 65, 132, 28, 154,
            153, 103, 215, 243, 30, 15, 209, 1,
        ]);

        assert_eq!(*sig.bytes(), true_sig);

        let collect_channel = msg.collect(paych_addr).unwrap();

        let sig = w
            .sign(&address, &collect_channel.to_signing_bytes())
            .unwrap();

        let true_sig = Vec::from([
            168, 247, 207, 223, 76, 179, 68, 6, 22, 225, 190, 167, 153, 26, 146, 206, 129, 104,
            183, 216, 83, 189, 10, 254, 254, 186, 224, 30, 8, 47, 247, 89, 2, 234, 30, 208, 162,
            179, 29, 147, 97, 250, 247, 198, 181, 204, 208, 177, 90, 199, 60, 29, 53, 12, 227, 14,
            32, 203, 27, 237, 51, 225, 52, 30, 0,
        ]);

        assert_eq!(*sig.bytes(), true_sig);
    }
}
