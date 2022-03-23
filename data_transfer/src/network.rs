use filecoin::{cid_helpers::CidCbor, types::Cbor};
use graphsync::traversal::{RecursionLimit, Selector};
use graphsync::Extensions;
use libipld::Cid;
use num_bigint::bigint_ser;
use num_bigint::{BigInt, ToBigInt};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};

pub static EXTENSION_KEY: &str = "fil/data-transfer/1.1";

#[derive(Debug, PartialEq, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u64)]
pub enum MessageType {
    New = 0,
    Update,
    Cancel,
    Complete,
    Voucher,
    VoucherResult,
    Restart,
    RestartExistingChannelRequest,
}

#[derive(Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, Clone, Hash)]
pub struct ChannelId {
    pub initiator: String,
    pub responder: String,
    pub id: u64,
}

impl Default for ChannelId {
    fn default() -> Self {
        Self {
            initiator: "".to_string(),
            responder: "".to_string(),
            id: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferRequest {
    #[serde(rename = "BCid")]
    pub root: CidCbor,
    #[serde(rename = "Type")]
    pub mtype: MessageType,
    #[serde(rename = "Paus")]
    pub pause: bool,
    #[serde(rename = "Part")]
    pub partial: bool,
    #[serde(rename = "Pull")]
    pub pull: bool,
    #[serde(rename = "Stor")]
    pub selector: Selector,
    #[serde(rename = "Vouch")]
    pub voucher: Option<DealProposal>,
    #[serde(rename = "VTyp")]
    pub voucher_type: String,
    #[serde(rename = "XferID")]
    pub transfer_id: u64,
    #[serde(rename = "RestartChannel")]
    pub restart_channel: ChannelId,
}

impl Default for TransferRequest {
    fn default() -> Self {
        let cid = CidCbor::from(Cid::default());
        Self {
            root: cid,
            mtype: MessageType::New,
            pause: false,
            partial: false,
            pull: true,
            selector: Selector::ExploreRecursive {
                limit: RecursionLimit::None,
                sequence: Box::new(Selector::ExploreAll {
                    next: Box::new(Selector::ExploreRecursiveEdge),
                }),
                current: None,
            },
            voucher: None,
            voucher_type: String::new(),
            transfer_id: 1,
            restart_channel: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferResponse {
    #[serde(rename = "Type")]
    pub mtype: MessageType,
    #[serde(rename = "Acpt")]
    pub accepted: bool,
    #[serde(rename = "Paus")]
    pub paused: bool,
    #[serde(rename = "XferID")]
    pub transfer_id: u64,
    #[serde(rename = "VRes")]
    pub voucher: Option<DealResponse>,
    #[serde(rename = "VTyp")]
    pub voucher_type: String,
}

impl Default for TransferResponse {
    fn default() -> Self {
        Self {
            mtype: MessageType::New,
            accepted: true,
            paused: false,
            transfer_id: 1,
            voucher: None,
            voucher_type: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TransferMessage {
    #[serde(default = "not_req")]
    pub is_rq: bool,
    pub request: Option<TransferRequest>,
    pub response: Option<TransferResponse>,
}

impl Cbor for TransferMessage {}

fn not_req() -> bool {
    false
}

impl From<()> for TransferMessage {
    fn from(_: ()) -> Self {
        Default::default()
    }
}

impl From<TransferMessage> for () {
    fn from(_: TransferMessage) -> Self {}
}

impl TryFrom<&Extensions> for TransferMessage {
    type Error = String;
    fn try_from(ext: &Extensions) -> Result<Self, Self::Error> {
        if let Some(data) = ext.get(EXTENSION_KEY) {
            TransferMessage::unmarshal_cbor(data).map_err(|e| e.to_string())
        } else {
            Err("No transfer message extension".to_string())
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u64)]
pub enum DealStatus {
    New = 0,
    Unsealing,
    Unsealed,
    WaitForAcceptance,
    PaymentChannelCreating,
    PaymentChannelAddingFunds,
    Accepted,
    FundsNeededUnseal,
    Failing,
    Rejected,
    FundsNeeded,
    SendFunds,
    SendFundsLastPayment,
    Ongoing,
    FundsNeededLastPayment,
    Completed,
    DealNotFound,
    Errored,
    BlocksComplete,
    Finalizing,
    Completing,
    CheckComplete,
    CheckFunds,
    InsufficientFunds,
    PaymentChannelAllocatingLane,
    Cancelling,
    Cancelled,
    WaitingForLastBlocks,
    PaymentChannelAddingInitialFunds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DealProposal {
    Pull {
        #[serde(rename = "PayloadCID")]
        payload_cid: CidCbor,
        #[serde(rename = "ID")]
        id: u64,
        #[serde(rename = "Params")]
        params: PullParams,
    },
    Push {
        #[serde(rename = "PayloadCID")]
        payload_cid: CidCbor,
        #[serde(rename = "ID")]
        id: u64,
        #[serde(rename = "Params")]
        params: PushParams,
    },
}

impl DealProposal {
    pub fn voucher_type(&self) -> String {
        match self {
            Self::Pull { .. } => "RetrievalDealProposal/1".to_string(),
            Self::Push { .. } => "PushProposal/1".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PullParams {
    pub selector: Option<Selector>,
    #[serde(rename = "PieceCID")]
    pub piece_cid: Option<CidCbor>,
    #[serde(with = "bigint_ser")]
    pub price_per_byte: BigInt,
    pub payment_interval: u64, // when to request payment
    pub payment_interval_increase: u64,
    #[serde(with = "bigint_ser")]
    pub unseal_price: BigInt,
}

impl Default for PullParams {
    fn default() -> Self {
        let zero: isize = 0;
        Self {
            selector: None,
            piece_cid: None,
            price_per_byte: zero.to_bigint().unwrap(),
            payment_interval: 100000,
            payment_interval_increase: 1000,
            unseal_price: zero.to_bigint().unwrap(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct PushParams {
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DealResponse {
    // response to a push request
    Push {
        #[serde(rename = "ID")]
        id: u64,
        #[serde(rename = "Status")]
        status: DealStatus,
        #[serde(rename = "Message")]
        message: String,
        #[serde(rename = "Secret")]
        secret: String,
    },
    // response to a pull request
    Pull {
        #[serde(rename = "ID")]
        id: u64,
        #[serde(rename = "Status")]
        status: DealStatus,
        #[serde(rename = "PaymentOwed")]
        #[serde(with = "bigint_ser")]
        payment_owed: BigInt,
        #[serde(rename = "Message")]
        message: String,
    },
}

impl DealResponse {
    pub fn voucher_type(&self) -> String {
        match self {
            Self::Pull { .. } => "RetrievalDealResponse/1".to_string(),
            Self::Push { .. } => "PushResponse/1".to_string(),
        }
    }
}

impl Cbor for DealResponse {}

#[cfg(test)]
mod tests {
    use super::*;
    use hex;

    #[test]
    fn it_decodes_request() {
        let msg_data = hex::decode("a36449735271f56752657175657374aa6442436964d82a5827000171a0e402200a2439495cfb5eafbb79669f644ca2c5a3d31b28e96c424cde5dd0e540a7d9486454797065006450617573f46450617274f46450756c6cf56453746f72a16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a065566f756368a36a5061796c6f6164434944d82a5827000171a0e402200a2439495cfb5eafbb79669f644ca2c5a3d31b28e96c424cde5dd0e540a7d9486249440166506172616d73a66853656c6563746f72a16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0685069656365434944f66c5072696365506572427974654200646f5061796d656e74496e74657276616c192710775061796d656e74496e74657276616c496e6372656173651903e86b556e7365616c50726963654064565479707752657472696576616c4465616c50726f706f73616c2f3166586665724944016e526573746172744368616e6e656c8360600068526573706f6e7365f6").unwrap();

        let msg = TransferMessage::unmarshal_cbor(&msg_data).unwrap();

        println!("request {:?}", msg);

        assert_eq!(msg.is_rq, true);

        let request = msg.request.unwrap();

        let cid = Cid::try_from("bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq")
            .unwrap();
        // We can recover the CID
        assert_eq!(request.root.to_cid().unwrap(), cid);
    }

    #[test]
    fn it_decodes_completed() {
        let msg_data = hex::decode("a36449735271f46752657175657374f668526573706f6e7365a66454797065036441637074f56450617573f4665866657249441b0000017b0bb0870d6456526573a4665374617475730f6249441b0000017b0bb0870d6b5061796d656e744f77656440674d6573736167656064565479707752657472696576616c4465616c526573706f6e73652f31").unwrap();

        let msg = TransferMessage::unmarshal_cbor(&msg_data).unwrap();

        assert_eq!(msg.is_rq, false);

        let response = msg.response.unwrap();
        assert_eq!(response.mtype, MessageType::Complete);
        if let Some(DealResponse::Pull { status, .. }) = response.voucher {
            assert_eq!(status, DealStatus::Completed);
        } else {
            panic!("decoded wrong voucher {:?}", response.voucher);
        }
    }
}
