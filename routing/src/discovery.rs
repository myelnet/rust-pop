use crate::utils::{peer_table_from_bytes, peer_table_to_bytes};
use async_std::io;
use async_trait::async_trait;
use filecoin::cid_helpers::CidCbor;
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
use serde_cbor::{from_slice, to_vec};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, RwLock,
};
use std::time::Duration;

pub type RequestId = i32;
pub type PeerTable = HashMap<PeerId, SmallVec<[Multiaddr; 6]>>;
pub type SerializablePeerTable = HashMap<Vec<u8>, Vec<Vec<u8>>>;

#[derive(Debug, Clone)]
pub struct HubDiscoveryProtocol;

impl ProtocolName for HubDiscoveryProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/myel/hub-discovery/0.1"
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
    type Protocol = HubDiscoveryProtocol;
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

#[derive(Debug, PartialEq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct DiscoveryRequest {
    pub id: RequestId,
    pub hub: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct DiscoveryResponse {
    pub source: Vec<u8>,
    pub id: RequestId,
    pub addresses: SerializablePeerTable,
    pub index_root: Option<CidCbor>,
}

pub struct HubDiscovery {
    //  we implement our own id_counter (instead of libp2p's) to ensure the request / response messages are CBOR encodable
    id_counter: Arc<AtomicI32>,
    inner: RequestResponse<DiscoveryCodec>,
    pub hub_table: Arc<RwLock<PeerTable>>,
    peer_id: PeerId,
    pub multiaddr: SmallVec<[Multiaddr; 6]>,
    hub: bool,
    index_root: Option<CidCbor>,
}

impl HubDiscovery {
    pub fn new(config: Config, peer_id: PeerId, hub: bool, index_root: Option<CidCbor>) -> Self {
        let protocols = std::iter::once((HubDiscoveryProtocol, ProtocolSupport::Full));
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let inner = RequestResponse::new(DiscoveryCodec::default(), protocols, rr_config);

        // if no hub table was passed then initialize a dummy one (mainly for testing purposes)
        let hub_table = Arc::new(RwLock::new(HashMap::new()));

        Self {
            id_counter: Arc::new(AtomicI32::new(1)),
            multiaddr: SmallVec::new(),
            inner,
            hub_table,
            peer_id,
            hub,
            index_root,
        }
    }
    // only hubs should track leaf nodes, they do so using the inner behaviour's table
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.inner.add_address(peer, address);
    }

    /// Removes an address of a peer previously added via `add_address`.
    // only hubs should track leaf nodes, they do so using the inner behaviour's table
    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        if self.hub {
            self.inner.remove_address(peer, address);
        }
    }

    pub fn update_index_root(&mut self, root: CidCbor) {
        self.index_root = Some(root);
    }

    // want this to be private as we want connections to trigger table swaps. There could be
    //  a situation where call to the request function itself triggers a connection
    // and then we have duplicate requests being made
    fn request(&mut self, peer_id: &PeerId) -> RequestId {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        self.inner
            .send_request(peer_id, DiscoveryRequest { id, hub: self.hub });
        id
    }
}

impl NetworkBehaviour for HubDiscovery {
    type ProtocolsHandler = <RequestResponse<DiscoveryCodec> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = DiscoveryEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.inner.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        // when connecting ask for hub table
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

        let mut lock = self.hub_table.write().unwrap();
        // if is in table then will be removed
        if let Some(p) = peer_id {
            lock.remove(&p);
        }

        self.inner.inject_dial_failure(peer_id, req_res, error)
    }

    fn inject_new_listen_addr(&mut self, _id: ListenerId, addr: &Multiaddr) {
        // include self in peer table for syncing
        self.multiaddr.push(addr.clone());
        if self.hub {
            self.hub_table
                .write()
                .unwrap()
                .entry(self.peer_id)
                .or_default()
                .push(addr.clone());
        }
    }

    fn inject_expired_listen_addr(&mut self, _id: ListenerId, addr: &Multiaddr) {
        // include self in peer table for syncing
        self.multiaddr.retain(|x| x.clone() != addr.clone());
        if self.hub {
            self.hub_table
                .write()
                .unwrap()
                .entry(self.peer_id)
                .or_default()
                .retain(|x| x.clone() != addr.clone());
        }
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        self.inner.inject_event(peer_id, conn, event)
    }
    fn poll(
        &mut self,
        ctx: &mut Context,
        pparams: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        while let Poll::Ready(event) = self.inner.poll(ctx, pparams) {
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
                        let new_addresses = peer_table_to_bytes(&self.hub_table.read().unwrap());

                        let msg = DiscoveryResponse {
                            source: self.peer_id.to_bytes(),
                            id: request.id,
                            addresses: new_addresses,
                            index_root: self.index_root.clone(),
                        };
                        self.inner.send_response(channel, msg).unwrap();
                    }
                    RequestResponseMessage::Response {
                        request_id: _,
                        response,
                    } => {
                        //  addresses only get returned on a successful response
                        let new_addresses = peer_table_from_bytes(&response.addresses);
                        //  update our local peer table
                        //  extend overwrites colliding keys (we assume inbound information is most up to date)
                        self.hub_table.write().unwrap().extend(new_addresses);

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
                        "hub discovery outbound failure {} {} {:?}",
                        peer, request_id, error
                    );
                }
                RequestResponseEvent::InboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    println!(
                        "hub discovery inbound failure {} {} {:?}",
                        peer, request_id, error
                    );
                }
                RequestResponseEvent::ResponseSent {
                    peer: _,
                    request_id: _,
                } => {}
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
        let req = DiscoveryRequest { id: 1, hub: true };

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
        addresses.insert(peer.clone(), multiaddr);
        let resp = DiscoveryResponse {
            source: peer,
            id: 1,
            addresses,
            index_root: None,
        };

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
        addr: Vec<Multiaddr>,
        swarm: Swarm<HubDiscovery>,
    }

    impl Peer {
        fn new(num_addreses: usize, is_hub: bool) -> Self {
            let (peer_id, trans) = mk_transport();
            let mut swarm = Swarm::new(
                trans,
                HubDiscovery::new(Config::default(), peer_id, is_hub, None),
                peer_id,
            );
            for _i in 0..num_addreses {
                Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            }
            while swarm.next().now_or_never().is_some() {}
            let addresses = Swarm::listeners(&swarm).map(|addr| addr.clone()).collect();
            let peer = Self {
                peer_id,
                addr: addresses,
                swarm,
            };
            return peer;
        }

        fn get_hub_table(&mut self) -> Arc<RwLock<PeerTable>> {
            return self.swarm.behaviour_mut().hub_table.clone();
        }

        fn add_address(&mut self, peer: &Peer) {
            for addr in peer.addr.clone() {
                self.swarm.behaviour_mut().add_address(&peer.peer_id, addr);
            }
        }

        fn swarm(&mut self) -> &mut Swarm<HubDiscovery> {
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
        //  will have empty hub table as not a hub
        let mut peer1 = Peer::new(7, false);
        // will have themselves in hub table
        let peer2 = Peer::new(1, true);
        let mut peer3 = Peer::new(3, true);

        println!("before {:?}", peer3.get_hub_table().read().unwrap());
        println!("before {:?}", peer1.get_hub_table().read().unwrap());

        // peer 3 knows peer 2 only and itself
        peer3.add_address(&peer2);
        // peer 1 knows peer 3 only and itself
        peer1.add_address(&peer2);

        //  print logs for peer 2
        let peer2id = peer2.spawn("peer2");

        peer3.swarm().dial(peer2id).unwrap();
        assert_response_ok(peer3.next().await, 1);
        peer1.swarm().dial(peer2id).unwrap();
        assert_response_ok(peer1.next().await, 1);

        println!("after {:?}", peer3.get_hub_table().read().unwrap());
        println!("after {:?}", peer1.get_hub_table().read().unwrap());

        let table1 = peer1.get_hub_table();
        let lock1 = table1.read().unwrap();

        let table3 = peer3.get_hub_table();
        let lock3 = table3.read().unwrap();

        let mut k1: Vec<&PeerId> = lock1.keys().collect();
        let mut k3: Vec<&PeerId> = lock3.keys().collect();

        assert_eq!(k1.sort(), k3.sort());

        let mut v1: Vec<&SmallVec<[Multiaddr; 6]>> = lock1.values().collect();
        let mut v3: Vec<&SmallVec<[Multiaddr; 6]>> = lock3.values().collect();

        assert_eq!(v1.sort(), v3.sort());
    }
}
