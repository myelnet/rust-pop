use crate::hub_discovery::SerializablePeerTable;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::core::{
    connection::ConnectionId, ConnectedPoint, InboundUpgrade, Multiaddr, OutboundUpgrade, PeerId,
    UpgradeInfo,
};
use libp2p::swarm::{
    IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler, OneShotHandler,
    PollParameters, ProtocolsHandler,
};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, VecDeque},
    io, iter,
    task::{Context, Poll},
};

#[derive(Debug, Clone, PartialEq, Serialize_tuple, Deserialize_tuple, Default)]
pub struct RoutingProposal {
    pub root: CidCbor,
    pub peers: Option<SerializablePeerTable>,
}

impl Cbor for RoutingProposal {}

impl From<()> for RoutingProposal {
    fn from(_: ()) -> Self {
        Default::default()
    }
}

impl From<RoutingProposal> for () {
    fn from(_: RoutingProposal) -> Self {}
}

impl UpgradeInfo for RoutingProposal {
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
    type Output = RoutingProposal;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await?;
            socket.close().await?;
            let message = RoutingProposal::unmarshal_cbor(&buf).map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "Failed to decode CBOR message")
            })?;
            Ok(message)
        })
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for RoutingProposal
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
    //  responds to incoming gossip messages so there's no equivalent request here
    Response(PeerId, RoutingProposal),
}

pub struct RoutingNetwork {
    pending_events: VecDeque<
        NetworkBehaviourAction<
            RoutingNetEvent,
            OneShotHandler<RoutingProtocol, RoutingProposal, RoutingProposal>,
        >,
    >,
    connected: HashMap<PeerId, SmallVec<[Connection; 2]>>,
    pending_outbound_requests: HashMap<PeerId, SmallVec<[RoutingProposal; 10]>>,
}

impl Default for RoutingNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingNetwork {
    pub fn new() -> Self {
        Self {
            pending_events: VecDeque::new(),
            connected: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
        }
    }

    pub fn send_message(&mut self, peer: &PeerId, message: RoutingProposal) {
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
        message: RoutingProposal,
    ) -> Option<RoutingProposal> {
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
    type ProtocolsHandler = OneShotHandler<RoutingProtocol, RoutingProposal, RoutingProposal>;

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

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::net::{TcpListener, TcpStream};
    use async_std::task;
    use futures::prelude::*;
    use hex;
    use libipld::Cid;
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
    fn it_decodes_proposal() {
        let msg_data = hex::decode("82d82a5827000171a0e402200a2439495cfb5eafbb79669f644ca2c5a3d31b28e96c424cde5dd0e540a7d948f6").unwrap();

        println!("msg_data {:?}", msg_data);

        let msg = RoutingProposal::unmarshal_cbor(&msg_data).unwrap();

        println!("request {:?}", msg);

        // assert_eq!(msg.is_rq, true);

        // let request = msg.request.unwrap();

        let cid = Cid::try_from("bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq")
            .unwrap();
        // We can recover the CID
        assert_eq!(msg.root.to_cid().unwrap(), cid);
    }

    #[async_std::test]
    async fn test_upgrade() {
        use libp2p::core::upgrade;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let server = async move {
            let incoming = listener.incoming().into_future().await.0.unwrap().unwrap();
            upgrade::apply_inbound(incoming, RoutingProtocol)
                .await
                .unwrap();
        };

        let client = async move {
            let stream = TcpStream::connect(&listener_addr).await.unwrap();
            upgrade::apply_outbound(stream, RoutingProposal::default(), upgrade::Version::V1)
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
        swarm: Swarm<RoutingNetwork>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let mut swarm = Swarm::new(trans, RoutingNetwork::new(), peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
            }
        }

        fn swarm(&mut self) -> &mut Swarm<RoutingNetwork> {
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

        async fn next(&mut self) -> Option<RoutingNetEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    return Some(event);
                }
            }
        }
    }

    fn assert_complete_ok(event: Option<RoutingNetEvent>, root: Cid) {
        if let Some(RoutingNetEvent::Response(_, resp)) = event {
            assert_eq!(resp.root.to_cid().unwrap(), root);
        } else {
            panic!("{:?} should not exist", event);
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
            RoutingProposal {
                root: CidCbor::from(cid),
                peers: None,
            },
        );

        peer2.spawn("peer2");

        assert_complete_ok(peer1.next().await, cid);
    }
}
