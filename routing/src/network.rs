use crate::hub_discovery::SerializablePeerTable;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libipld::Cid;
use libp2p::core::{
    connection::ConnectionId, ConnectedPoint, InboundUpgrade, Multiaddr, OutboundUpgrade, PeerId,
    UpgradeInfo,
};
use libp2p::swarm::{
    IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, OneShotHandler,
    PollParameters, ProtocolsHandler,
};
// use serde::{Deserialize, Serialize};
use serde::{Deserialize, Serialize};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, VecDeque},
    io, iter,
    task::{Context, Poll},
};

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

#[derive(Debug, Clone, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct PeersforContent {
    pub root: CidCbor,
    pub peers: Option<SerializablePeerTable>,
}

impl Cbor for PeersforContent {}

impl Default for PeersforContent {
    fn default() -> Self {
        Self {
            root: CidCbor::default(),
            peers: None,
        }
    }
}

impl From<()> for PeersforContent {
    fn from(_: ()) -> Self {
        Default::default()
    }
}

impl From<PeersforContent> for () {
    fn from(_: PeersforContent) -> Self {
        ()
    }
}

impl UpgradeInfo for PeersforContent {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/myel/content-routing/0.1")
    }
}

#[derive(Clone, Debug, Default)]
pub struct RoutingProtocol;

impl UpgradeInfo for RoutingProtocol {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/myel/content-routing/0.1")
    }
}

impl<TSocket> InboundUpgrade<TSocket> for RoutingProtocol
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = PeersforContent;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await?;
            socket.close().await?;
            let message = PeersforContent::unmarshal_cbor(&buf).map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "Failed to decode CBOR message")
            })?;
            Ok(message)
        })
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for PeersforContent
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
pub enum RoutingNetEvent {
    //  respond to gossip messages so there's no equivalent request here
    Response(PeerId, PeersforContent),
}

pub struct RoutingNetwork {
    pending_events: VecDeque<
        NetworkBehaviourAction<
            RoutingNetEvent,
            OneShotHandler<RoutingProtocol, PeersforContent, PeersforContent>,
        >,
    >,
    connected: HashMap<PeerId, SmallVec<[Connection; 2]>>,
    pending_outbound_requests: HashMap<PeerId, SmallVec<[PeersforContent; 10]>>,
}

impl RoutingNetwork {
    pub fn new() -> Self {
        Self {
            pending_events: VecDeque::new(),
            connected: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
        }
    }

    pub fn send_message(&mut self, peer: &PeerId, message: PeersforContent) {
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
        message: PeersforContent,
    ) -> Option<PeersforContent> {
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

impl NetworkBehaviour for RoutingNetwork {
    type ProtocolsHandler = OneShotHandler<RoutingProtocol, PeersforContent, PeersforContent>;

    type OutEvent = RoutingNetEvent;

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
        self.pending_events
            .push_back(NetworkBehaviourAction::GenerateEvent(
                RoutingNetEvent::Response(peer_id, event),
            ));
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
