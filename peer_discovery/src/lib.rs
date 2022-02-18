use async_std::io;
use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncWrite};
use futures::prelude::*;
use futures::task::{Context, Poll};
use libp2p::core::connection::{ConnectionId, ListenerId};
use libp2p::core::{upgrade, ConnectedPoint, Multiaddr, PeerId};
use libp2p::request_response::{
    ProtocolName, ProtocolSupport, RequestResponse, RequestResponseCodec, RequestResponseConfig,
    RequestResponseEvent, RequestResponseMessage,
};
use libp2p::swarm::{
    DialError, IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    ProtocolsHandler,
};
use serde::{Deserialize, Serialize};
use serde_cbor::{from_slice, to_vec};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, RwLock,
};
use std::time::Duration;

pub type RequestId = i32;
pub type PeerTable = HashMap<PeerId, SmallVec<[Multiaddr; 6]>>;
pub type SerializablePeerTable = HashMap<Vec<u8>, Vec<Vec<u8>>>;

#[derive(Debug, Clone)]
pub struct PeerDiscoveryProtocol;

impl ProtocolName for PeerDiscoveryProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/myel/peer-discovery/0.1"
    }
}

#[derive(Clone)]
pub struct DiscoveryCodec {
    max_msg_size: usize,
}

impl Default for DiscoveryCodec {
    fn default() -> Self {
        Self {
            max_msg_size: 1000000,
        }
    }
}

#[async_trait]
impl RequestResponseCodec for DiscoveryCodec {
    type Protocol = PeerDiscoveryProtocol;
    type Response = DiscoveryResponse;
    type Request = DiscoveryRequest;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Send + Unpin,
    {
        let packet = upgrade::read_length_prefixed(io, self.max_msg_size)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if packet.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "End of output"));
        }

        let message = from_slice(&packet).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(message)
    }
    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        request: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        let bytes = to_vec(&request).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        upgrade::write_length_prefixed(io, bytes).await?;
        Ok(())
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = upgrade::read_length_prefixed(io, self.max_msg_size).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        let message = from_slice(&vec).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(message)
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let bytes = to_vec(&res).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        upgrade::write_length_prefixed(io, bytes).await?;

        io.close().await?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum DiscoveryEvent {
    ResponseReceived(DiscoveryResponse),
}

#[derive(Clone)]
pub struct Config {
    /// Timeout of a request.
    pub request_timeout: Duration,
    /// Time a connection is kept alive.
    pub connection_keep_alive: Duration,
}

impl Config {
    pub fn new() -> Self {
        Self {
            request_timeout: Duration::from_secs(10 * 60),
            connection_keep_alive: Duration::from_secs(10 * 60),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DiscoveryRequest {
    pub id: RequestId,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DiscoveryResponse {
    pub id: RequestId,
    pub addresses: SerializablePeerTable,
}
pub struct PeerDiscovery {
    //  we implement our own id_counter (instead of libp2p's) to ensure the request / response messages are CBOR encodable
    id_counter: Arc<AtomicI32>,
    inner: RequestResponse<DiscoveryCodec>,
    peer_table: Arc<RwLock<PeerTable>>,
    peer_id: PeerId,
}

impl PeerDiscovery {
    pub fn new(config: Config, peer_table: Arc<RwLock<PeerTable>>, peer_id: PeerId) -> Self {
        let protocols = std::iter::once((PeerDiscoveryProtocol, ProtocolSupport::Full));
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let inner = RequestResponse::new(DiscoveryCodec::default(), protocols, rr_config);
        Self {
            id_counter: Arc::new(AtomicI32::new(1)),
            inner,
            peer_table,
            peer_id,
        }
    }
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.peer_table
            .write()
            .unwrap()
            .entry(*peer)
            .or_default()
            .push(address.clone());
        self.inner.add_address(peer, address);
    }

    /// Removes an address of a peer previously added via `add_address`.
    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        let mut last = false;
        if let Some(addresses) = self.peer_table.write().unwrap().get_mut(peer) {
            addresses.retain(|a| a != address);
            last = addresses.is_empty();
        }
        if last {
            self.peer_table.write().unwrap().remove(peer);
        }
        self.inner.remove_address(peer, address);
    }

    // want this to be private as we want connections to trigger table swaps. There could be
    //  a situation where call to the request function itself triggers a connection and then we have duplicate requests
    // being made
    fn request(&mut self, peer_id: &PeerId) -> RequestId {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        self.inner.send_request(&peer_id, DiscoveryRequest { id });
        id
    }
}

impl NetworkBehaviour for PeerDiscovery {
    type ProtocolsHandler = <RequestResponse<DiscoveryCodec> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = DiscoveryEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.inner.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        // when connecting immediately swap peer tables
        self.request(peer_id);
        self.inner.inject_connected(peer_id)
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        self.inner.inject_disconnected(peer_id);
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        failed_addressses: Option<&Vec<Multiaddr>>,
    ) {
        self.inner
            .inject_connection_established(peer_id, conn, endpoint, failed_addressses)
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        handler: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
    ) {
        let req_res = handler;
        self.inner
            .inject_connection_closed(peer_id, conn, endpoint, req_res)
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
        error: &DialError,
    ) {
        let req_res = handler;
        self.inner.inject_dial_failure(peer_id, req_res, error)
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        // initialize table with self
        self.peer_table
            .write()
            .unwrap()
            .entry(self.peer_id)
            .or_default()
            .push(addr.clone());
        self.inner.inject_new_listen_addr(id, addr)
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.inner.inject_expired_listen_addr(id, addr)
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_new_external_addr(addr)
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_expired_external_addr(addr)
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn Error + 'static)) {
        self.inner.inject_listener_error(id, err)
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.inner.inject_listener_closed(id, reason)
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        return self.inner.inject_event(peer_id, conn, event);
    }
    fn poll(
        &mut self,
        ctx: &mut Context,
        pparams: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        let mut exit = false;
        while !exit {
            exit = true;
            while let Poll::Ready(event) = self.inner.poll(ctx, pparams) {
                exit = false;
                let event = match event {
                    NetworkBehaviourAction::GenerateEvent(event) => event,
                    NetworkBehaviourAction::Dial { opts, handler } => {
                        return Poll::Ready(NetworkBehaviourAction::Dial { opts, handler });
                    }
                    NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event,
                    } => {
                        return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                            peer_id,
                            handler,
                            event,
                        });
                    }
                    NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                        return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                            address,
                            score,
                        });
                    }
                    NetworkBehaviourAction::CloseConnection {
                        peer_id,
                        connection,
                    } => {
                        return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                            peer_id,
                            connection,
                        });
                    }
                };
                match event {
                    RequestResponseEvent::Message { peer: _, message } => match message {
                        RequestResponseMessage::Request {
                            request_id: _,
                            request,
                            channel,
                        } => {
                            // validate request
                            let new_addresses: HashMap<Vec<u8>, Vec<Vec<u8>>> = self
                                .peer_table
                                .read()
                                .unwrap()
                                .iter()
                                .map_while(|(peer, addresses)| {
                                    let mut addr_vec = Vec::new();
                                    for addr in addresses {
                                        addr_vec.push((*addr).to_vec())
                                    }
                                    Some((peer.to_bytes(), addr_vec))
                                })
                                .collect();

                            let msg = DiscoveryResponse {
                                id: request.id,
                                addresses: new_addresses,
                            };
                            self.inner.send_response(channel, msg).unwrap();
                        }
                        RequestResponseMessage::Response {
                            request_id: _,
                            response,
                        } => {
                            //  addresses only get returned on a successful response
                            let new_addresses: PeerTable = response
                                .clone()
                                .addresses
                                .iter()
                                .map_while(|(peer, addresses)| {
                                    let mut addr_vec = SmallVec::<[Multiaddr; 6]>::new();
                                    for addr in addresses {
                                        addr_vec.push(Multiaddr::try_from(addr.clone()).unwrap())
                                    }
                                    Some((PeerId::from_bytes(peer).unwrap(), addr_vec))
                                })
                                .collect();
                            //  update our local peer table
                            self.peer_table.write().unwrap().extend(new_addresses);

                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                                DiscoveryEvent::ResponseReceived(response),
                            ));
                        }
                    },
                    RequestResponseEvent::OutboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        println!(
                            "peer discovery outbound failure {} {} {:?}",
                            peer, request_id, error
                        );
                    }
                    RequestResponseEvent::InboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        println!(
                            "peer discovery inbound failure {} {} {:?}",
                            peer, request_id, error
                        );
                    }
                    RequestResponseEvent::ResponseSent {
                        peer: _,
                        request_id: _,
                    } => {}
                }
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{Multiaddr, PeerId, Swarm, Transport};
    use serde_cbor::{from_slice, to_vec};

    #[test]
    fn request_serialization() {
        let req = DiscoveryRequest { id: 1 };

        let msgc = req.clone();
        let msg_encoded = to_vec(&req).unwrap();

        let des_msg = from_slice(&msg_encoded).unwrap();

        assert_eq!(msgc, des_msg)
    }

    #[test]
    fn response_serialization() {
        let multiaddr = Vec::from(["/ip4/127.0.0.1/tcp/0".as_bytes().to_vec()]);
        let peer = "/ip4/127.0.0.1/tcp/0".as_bytes().to_vec();
        let mut addresses = HashMap::new();
        addresses.insert(peer, multiaddr);
        let resp = DiscoveryResponse { id: 1, addresses };

        let msgc = resp.clone();
        let msg_encoded = to_vec(&resp).unwrap();

        let des_msg = from_slice(&msg_encoded).unwrap();

        assert_eq!(msgc, des_msg)
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
        swarm: Swarm<PeerDiscovery>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let peer_table = Arc::new(RwLock::new(HashMap::new()));
            let mut swarm = Swarm::new(
                trans,
                PeerDiscovery::new(Config::default(), peer_table, peer_id),
                peer_id,
            );
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            let peer = Self {
                peer_id,
                addr: addr.clone(),
                swarm,
            };
            return peer;
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn swarm(&mut self) -> &mut Swarm<PeerDiscovery> {
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

        async fn next(&mut self) -> Option<DiscoveryEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    return Some(event);
                }
            }
        }
    }

    fn assert_response_ok(event: Option<DiscoveryEvent>, id: RequestId) {
        if let Some(DiscoveryEvent::ResponseReceived(responses)) = event {
            assert_eq!(responses.id, id);
        } else {
            panic!("{:?} is not a response event", event);
        }
    }

    #[async_std::test]
    async fn test_request() {
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        let mut peer3 = Peer::new();

        println!("{:?}", peer3.swarm().behaviour().peer_table.read().unwrap());
        println!("{:?}", peer1.swarm().behaviour().peer_table.read().unwrap());

        // peer 2 knows everyone
        peer2.add_address(&peer1);
        peer2.add_address(&peer3);
        // peer 3 knows peer 2 only and itself
        peer3.add_address(&peer2);
        // peer 1 knows peer 2 only and itself
        peer1.add_address(&peer2);

        //  print logs for peer 2
        let peer2id = peer2.spawn("peer2");

        peer3.swarm().dial(peer2id).unwrap();
        peer1.swarm().dial(peer2id).unwrap();

        assert_response_ok(peer3.next().await, 1);
        assert_response_ok(peer1.next().await, 1);

        println!("{:?}", peer3.swarm().behaviour().peer_table.read().unwrap());
        println!("{:?}", peer1.swarm().behaviour().peer_table.read().unwrap());

        assert_eq!(
            *peer3.swarm().behaviour().peer_table.read().unwrap(),
            *peer1.swarm().behaviour().peer_table.read().unwrap()
        );
    }
}
