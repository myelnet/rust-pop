use filecoin::{cid_helpers::CidCbor, types::Cbor};
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use graphsync::traversal::{RecursionLimit, Selector};
use libipld::Cid;
use libp2p::core::{upgrade, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use num_bigint::bigint_ser;
use num_bigint::{BigInt, ToBigInt};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::{io, iter};

#[derive(Debug, PartialEq, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u64)]
pub enum MessageType {
    NewMessage = 0,
    UpdateMessage,
    CancelMessage,
    CompleteMessage,
    VoucherMessage,
    VoucherResultMessage,
    RestartMessage,
    RestartExistingChannelRequestMessage,
}

#[derive(Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple, Clone, Hash)]
pub struct ChannelId {
    initiator: String,
    responder: String,
    id: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferRequest {
    #[serde(rename = "BCid")]
    root: CidCbor,
    #[serde(rename = "Type")]
    mtype: MessageType,
    #[serde(rename = "Paus")]
    pause: bool,
    #[serde(rename = "Part")]
    partial: bool,
    #[serde(rename = "Pull")]
    pull: bool,
    #[serde(rename = "Stor")]
    selector: Selector,
    #[serde(rename = "Vouch")]
    voucher: DealProposal,
    #[serde(rename = "VTyp")]
    voucher_type: String,
    #[serde(rename = "XferID")]
    transfer_id: u64,
    #[serde(rename = "RestartChannel")]
    restart_channel: Option<ChannelId>,
}

impl Default for TransferRequest {
    fn default() -> Self {
        let cid = CidCbor::from(Cid::default());
        let voucher = DealProposal {
            payload_cid: cid.clone(),
            id: 1,
            params: Default::default(),
        };
        Self {
            root: cid.clone(),
            mtype: MessageType::NewMessage,
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
            voucher,
            voucher_type: DealProposal::voucher_type(),
            transfer_id: 1,
            restart_channel: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferResponse {
    #[serde(rename = "Type")]
    mtype: MessageType,
    #[serde(rename = "Acpt")]
    accepted: bool,
    #[serde(rename = "Paus")]
    paused: bool,
    #[serde(rename = "XferID")]
    transfer_id: u64,
    #[serde(rename = "VRes")]
    voucher: DealResponse,
    #[serde(rename = "VTyp")]
    voucher_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TransferMessage {
    #[serde(default = "not_req")]
    is_rq: bool,
    request: Option<TransferRequest>,
    response: Option<TransferResponse>,
}

impl Cbor for TransferMessage {}

fn not_req() -> bool {
    false
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
pub struct DealProposal {
    #[serde(rename = "PayloadCID")]
    payload_cid: CidCbor,
    #[serde(rename = "ID")]
    id: u64,
    #[serde(rename = "Params")]
    params: DealParams,
}

impl DealProposal {
    pub fn voucher_type() -> String {
        "RetrievalDealProposal/1".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DealParams {
    selector: Option<Selector>,
    #[serde(rename = "PieceCID")]
    piece_cid: Option<CidCbor>,
    #[serde(with = "bigint_ser")]
    price_per_byte: BigInt,
    payment_interval: u64, // when to request payment
    payment_interval_increase: u64,
    #[serde(with = "bigint_ser")]
    unseal_price: BigInt,
}

impl Default for DealParams {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DealResponse {
    #[serde(rename = "ID")]
    id: u64,
    #[serde(rename = "Status")]
    status: DealStatus,
    #[serde(rename = "PaymentOwed")]
    #[serde(with = "bigint_ser")]
    payment_owed: BigInt,
    #[serde(rename = "Message")]
    message: String,
}

#[derive(Clone, Debug, Default)]
pub struct TransferProtocol;

impl UpgradeInfo for TransferProtocol {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/fil/datatransfer/1.2.0")
    }
}

impl<TSocket> InboundUpgrade<TSocket> for TransferProtocol
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = TransferMessage;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await?;
            let message = TransferMessage::unmarshal_cbor(&buf).map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "Failed to decode CBOR message")
            })?;
            socket.close().await?;
            Ok(message)
        })
    }
}

impl UpgradeInfo for TransferMessage {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/fil/datatransfer/1.2.0")
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for TransferMessage
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let mut buf = self
                .marshal_cbor()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            socket.write_all(&mut buf).await?;
            socket.close().await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::net::{TcpListener, TcpStream};
    use futures::prelude::*;
    use graphsync::traversal::RecursionLimit;
    use hex;

    #[test]
    fn it_decodes_request() {
        let msg_data = hex::decode("a36449735271f56752657175657374aa6442436964d82a5827000171a0e402200a2439495cfb5eafbb79669f644ca2c5a3d31b28e96c424cde5dd0e540a7d9486454797065006450617573f46450617274f46450756c6cf56453746f72a16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a065566f756368a36a5061796c6f6164434944d82a5827000171a0e402200a2439495cfb5eafbb79669f644ca2c5a3d31b28e96c424cde5dd0e540a7d9486249440166506172616d73a66853656c6563746f72a16152a2616ca1646e6f6e65a0623a3ea16161a1613ea16140a0685069656365434944f66c5072696365506572427974654200646f5061796d656e74496e74657276616c192710775061796d656e74496e74657276616c496e6372656173651903e86b556e7365616c50726963654064565479707752657472696576616c4465616c50726f706f73616c2f3166586665724944016e526573746172744368616e6e656c8360600068526573706f6e7365f6").unwrap();

        let msg = TransferMessage::unmarshal_cbor(&msg_data).unwrap();

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
        assert_eq!(response.mtype, MessageType::CompleteMessage);
        assert_eq!(response.voucher.status, DealStatus::Completed);
    }

    #[async_std::test]
    async fn test_upgrade() {
        use libp2p::core::upgrade;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let server = async move {
            let incoming = listener.incoming().into_future().await.0.unwrap().unwrap();
            upgrade::apply_inbound(incoming, TransferProtocol)
                .await
                .unwrap();
        };

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let client = async move {
            let stream = TcpStream::connect(&listener_addr).await.unwrap();
            upgrade::apply_outbound(
                stream,
                TransferMessage {
                    is_rq: true,
                    request: Some(TransferRequest {
                        root: CidCbor::from(Cid::default()),
                        mtype: MessageType::NewMessage,
                        ..Default::default()
                    }),
                    response: None,
                },
                upgrade::Version::V1,
            )
            .await
            .unwrap();
        };

        future::select(Box::pin(server), Box::pin(client)).await;
    }
}
