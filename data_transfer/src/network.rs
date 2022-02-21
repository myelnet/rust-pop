use filecoin::{
    cid_helpers::CidCbor,
    types::{Cbor},
};
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use graphsync::traversal::{RecursionLimit, Selector};
use graphsync::Extensions;
use libipld::Cid;
use libp2p::core::{
    connection::ConnectionId, ConnectedPoint, InboundUpgrade, Multiaddr, OutboundUpgrade, PeerId,
    UpgradeInfo,
};
use libp2p::swarm::{
    IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, OneShotHandler,
    PollParameters, ProtocolsHandler,
};
use num_bigint::bigint_ser;
use num_bigint::{BigInt, ToBigInt};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, VecDeque},
    io, iter,
    task::{Context, Poll},
};

pub static EXTENSION_KEY: &str = "fil/data-transfer/1.1";

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
            voucher: Some(voucher),
            voucher_type: DealProposal::voucher_type(),
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
            mtype: MessageType::NewMessage,
            accepted: true,
            paused: false,
            transfer_id: 1,
            voucher: None,
            voucher_type: format!(""),
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
    fn from(_: TransferMessage) -> Self {
        ()
    }
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
pub struct DealProposal {
    #[serde(rename = "PayloadCID")]
    pub payload_cid: CidCbor,
    #[serde(rename = "ID")]
    pub id: u64,
    #[serde(rename = "Params")]
    pub params: DealParams,
}

impl DealProposal {
    pub fn voucher_type() -> String {
        "RetrievalDealProposal/1".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DealParams {
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
    pub id: u64,
    #[serde(rename = "Status")]
    pub status: DealStatus,
    #[serde(rename = "PaymentOwed")]
    #[serde(with = "bigint_ser")]
    pub payment_owed: BigInt,
    #[serde(rename = "Message")]
    pub message: String,
}

impl DealResponse {
    pub fn voucher_type() -> String {
        "RetrievalDealResponse/1".to_string()
    }
}

impl Cbor for DealResponse {}

#[derive(Clone, Debug, Default)]
pub struct TransferProtocol;

impl UpgradeInfo for TransferProtocol {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/fil/datatransfer/1.1.0")
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
            socket.close().await?;
            let message = TransferMessage::unmarshal_cbor(&buf).map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "Failed to decode CBOR message")
            })?;
            Ok(message)
        })
    }
}

impl UpgradeInfo for TransferMessage {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/fil/datatransfer/1.1.0")
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

pub const EMPTY_QUEUE_SHRINK_THRESHOLD: usize = 100;

#[derive(Debug)]
pub enum DtNetEvent {
    Request(PeerId, TransferRequest),
    Response(PeerId, TransferResponse),
}

pub struct DataTransferNetwork {
    pending_events: VecDeque<
        NetworkBehaviourAction<
            DtNetEvent,
            OneShotHandler<TransferProtocol, TransferMessage, TransferMessage>,
        >,
    >,
    connected: HashMap<PeerId, SmallVec<[Connection; 2]>>,
    pending_outbound_requests: HashMap<PeerId, SmallVec<[TransferMessage; 10]>>,
}

impl DataTransferNetwork {
    pub fn new() -> Self {
        Self {
            pending_events: VecDeque::new(),
            connected: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
        }
    }

    pub fn send_message(&mut self, peer: &PeerId, message: TransferMessage) {
        if let Some(message) = self.try_send_message(peer, message) {
            self.pending_outbound_requests
                .entry(*peer)
                .or_default()
                .push(message);
        }
    }

    fn try_send_message(
        &mut self,
        peer: &PeerId,
        message: TransferMessage,
    ) -> Option<TransferMessage> {
        if let Some(connections) = self.connected.get_mut(peer) {
            if connections.is_empty() {
                return Some(message);
            }
            self.pending_events
                .push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: *peer,
                    handler: NotifyHandler::Any,
                    event: message,
                });
            None
        } else {
            Some(message)
        }
    }
}

impl NetworkBehaviour for DataTransferNetwork {
    type ProtocolsHandler = OneShotHandler<TransferProtocol, TransferMessage, TransferMessage>;

    type OutEvent = DtNetEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        OneShotHandler::default()
    }

    fn addresses_of_peer(&mut self, peer: &PeerId) -> Vec<Multiaddr> {
        let mut addresses = Vec::new();
        if let Some(connections) = self.connected.get(peer) {
            addresses.extend(connections.iter().filter_map(|c| c.address.clone()));
        }
        addresses
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        _: ConnectionId,
        event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        // @FIXME: sometimes we seem to receive empty messages. It might be a bug from the handler.
        if let (None, None) = (&event.request, &event.response) {
            return;
        }
        if event.is_rq {
            self.pending_events
                .push_back(NetworkBehaviourAction::GenerateEvent(DtNetEvent::Request(
                    peer_id,
                    event
                        .request
                        .expect("Expected event request to have request field"),
                )));
        } else {
            self.pending_events
                .push_back(NetworkBehaviourAction::GenerateEvent(DtNetEvent::Response(
                    peer_id,
                    event
                        .response
                        .expect("Expected event response to have a response field"),
                )));
        }
    }

    fn inject_address_change(
        &mut self,
        peer: &PeerId,
        conn: &ConnectionId,
        _old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        let new_address = match new {
            ConnectedPoint::Dialer { address, .. } => Some(address.clone()),
            ConnectedPoint::Listener { .. } => None,
        };
        let connections = self
            .connected
            .get_mut(peer)
            .expect("Address change can only happen on an established connection.");

        let connection = connections
            .iter_mut()
            .find(|c| &c.id == conn)
            .expect("Address change can only happen on an established connection.");
        connection.address = new_address;
    }

    fn inject_connection_established(
        &mut self,
        peer: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        _errors: Option<&Vec<Multiaddr>>,
    ) {
        let address = match endpoint {
            ConnectedPoint::Dialer { address, .. } => Some(address.clone()),
            ConnectedPoint::Listener { .. } => None,
        };
        self.connected
            .entry(*peer)
            .or_default()
            .push(Connection::new(*conn, address));
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        _: &ConnectedPoint,
        _: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
    ) {
        let connections = self
            .connected
            .get_mut(peer_id)
            .expect("Expected some established connection to peer before closing.");

        connections
            .iter()
            .position(|c| &c.id == conn)
            .map(|p: usize| connections.remove(p))
            .expect("Expected connection to be established before closing.");

        if connections.is_empty() {
            self.connected.remove(peer_id);
        }
    }

    fn inject_connected(&mut self, peer: &PeerId) {
        if let Some(pending) = self.pending_outbound_requests.remove(peer) {
            for request in pending {
                let msg = self.try_send_message(peer, request);
                assert!(msg.is_none());
            }
        }
    }

    fn inject_disconnected(&mut self, peer: &PeerId) {
        self.connected.remove(peer);
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        if let Some(ev) = self.pending_events.pop_front() {
            return Poll::Ready(ev);
        } else if self.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.pending_events.shrink_to_fit();
        }
        Poll::Pending
    }
}

/// Internal information tracked for an established connection.
struct Connection {
    id: ConnectionId,
    address: Option<Multiaddr>,
}

impl Connection {
    fn new(id: ConnectionId, address: Option<Multiaddr>) -> Self {
        Self { id, address }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::net::{TcpListener, TcpStream};
    use async_std::task;
    use futures::prelude::*;
    use hex;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use std::time::Duration;

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
        assert_eq!(response.mtype, MessageType::CompleteMessage);
        assert_eq!(response.voucher.unwrap().status, DealStatus::Completed);
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

    fn mk_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox)>) {
        let id_key = identity::Keypair::generate_ed25519();
        let peer_id = id_key.public().to_peer_id();
        let dh_key = Keypair::<X25519Spec>::new()
            .into_authentic(&id_key)
            .unwrap();
        let noise = NoiseConfig::xx(dh_key).into_authenticated();

        let transport = TcpConfig::new()
            .nodelay(true)
            .upgrade(libp2p::core::upgrade::Version::V1)
            .authenticate(noise)
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .boxed();
        (peer_id, transport)
    }

    struct Peer {
        peer_id: PeerId,
        addr: Multiaddr,
        swarm: Swarm<DataTransferNetwork>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let mut swarm = Swarm::new(trans, DataTransferNetwork::new(), peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
            }
        }

        fn swarm(&mut self) -> &mut Swarm<DataTransferNetwork> {
            &mut self.swarm
        }

        fn spawn(mut self, _name: &'static str) -> PeerId {
            let peer_id = self.peer_id;
            task::spawn(async move {
                loop {
                    let event = self.swarm.next().await;
                    println!("event {:?}", event);
                }
            });
            peer_id
        }

        async fn next(&mut self) -> Option<DtNetEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    return Some(event);
                }
            }
        }
    }

    fn assert_complete_ok(event: Option<DtNetEvent>, root: Cid) {
        if let Some(DtNetEvent::Request(_, req)) = event {
            assert_eq!(req.root.to_cid().unwrap(), root);
        } else {
            panic!("{:?} is not a complete event", event);
        }
    }

    #[async_std::test]
    async fn send_message() {
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();

        peer2.swarm().dial(peer1.addr.clone()).unwrap();

        let cid = Cid::default();

        peer2.swarm().behaviour_mut().send_message(
            &peer1.peer_id,
            TransferMessage {
                is_rq: true,
                request: Some(TransferRequest {
                    root: CidCbor::from(cid.clone()),
                    mtype: MessageType::NewMessage,
                    ..Default::default()
                }),
                response: None,
            },
        );
        peer2.spawn("peer2");
        assert_complete_ok(peer1.next().await, cid);
    }
}
