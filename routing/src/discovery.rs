use crate::cbor_helpers::PeerIdCbor;
use crate::index::ShrinkableMap;
use async_std::io;
use async_trait::async_trait;
use blockstore::types::BlockStore;
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
    Arc,
};
use std::time::Duration;

pub type RequestId = i32;
pub type PeerTable = HashMap<PeerIdCbor, Vec<Multiaddr>>;

impl ShrinkableMap<PeerIdCbor, Vec<Multiaddr>> for PeerTable {
    fn remove(&mut self, k: &PeerIdCbor) -> Option<Vec<Multiaddr>> {
        self.remove(k)
    }
    fn contains_key(&self, k: &PeerIdCbor) -> bool {
        self.contains_key(k)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryProtocol;

impl ProtocolName for DiscoveryProtocol {
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
    type Protocol = DiscoveryProtocol;
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
    // peer and index root
    ResponseReceived(RequestId, PeerId, Option<CidCbor>, Option<Multiaddr>),
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
    pub is_hub: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct DiscoveryResponse {
    // pub source: Vec<u8>,
    pub id: RequestId,
    pub hub_table: Option<PeerTable>,
    pub index_root: Option<CidCbor>,
    pub peer_ma: Option<Multiaddr>,
}

pub struct Discovery<B: BlockStore> {
    //  we implement our own id_counter (instead of libp2p's) to ensure the request / response messages are CBOR encodable
    id_counter: Arc<AtomicI32>,
    inner: RequestResponse<DiscoveryCodec>,
    pub hub_table: PeerTable,
    peer_id: PeerIdCbor,
    pub multiaddr: SmallVec<[Multiaddr; 4]>,
    is_hub: bool,
    store: Arc<B>,
}

impl<B: BlockStore> Discovery<B> {
    pub fn new(config: Config, peer_id: PeerId, is_hub: bool, store: Arc<B>) -> Self {
        let protocols = std::iter::once((DiscoveryProtocol, ProtocolSupport::Full));
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let inner = RequestResponse::new(DiscoveryCodec::default(), protocols, rr_config);

        Self {
            id_counter: Arc::new(AtomicI32::new(1)),
            multiaddr: SmallVec::new(),
            inner,
            hub_table: HashMap::new(),
            peer_id: PeerIdCbor::from(peer_id),
            is_hub,
            store,
        }
    }
    // only hubs should track leaf nodes, they do so using the inner behaviour's table
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.inner.add_address(peer, address);
    }

    /// Removes an address of a peer previously added via `add_address`.
    // only hubs should track leaf nodes, they do so using the inner behaviour's table
    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        if self.is_hub {
            self.inner.remove_address(peer, address);
        }
    }

    // want this to be private as we want connections to trigger table swaps. There could be
    //  a situation where call to the request function itself triggers a connection
    // and then we have duplicate requests being made
    fn request(&mut self, peer_id: &PeerId) -> RequestId {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        self.inner.send_request(
            peer_id,
            DiscoveryRequest {
                id,
                is_hub: self.is_hub,
            },
        );
        id
    }
}

impl<B: 'static + BlockStore> NetworkBehaviour for Discovery<B> {
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

        // if is in table then will be removed
        if let Some(p) = peer_id {
            self.hub_table.remove(&PeerIdCbor::from(p));
        }

        self.inner.inject_dial_failure(peer_id, req_res, error)
    }

    fn inject_new_listen_addr(&mut self, _id: ListenerId, addr: &Multiaddr) {
        // include self in peer table for syncing
        let ma = addr.clone();
        self.multiaddr.push(ma.clone());
        if self.is_hub {
            self.hub_table
                .entry(self.peer_id.clone())
                .or_default()
                .push(ma);
        }
    }

    fn inject_expired_listen_addr(&mut self, _id: ListenerId, addr: &Multiaddr) {
        // include self in peer table for syncing
        let ma = addr.clone();
        self.multiaddr.retain(|x| x.clone() != ma);
        if self.is_hub {
            self.hub_table
                .entry(self.peer_id.clone())
                .or_default()
                .retain(|x| x.clone() != ma);
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
                RequestResponseEvent::Message { peer, message } => match message {
                    RequestResponseMessage::Request {
                        request_id: _,
                        request,
                        channel,
                    } => {
                        // let index_root =
                        let hub_table = match self.is_hub {
                            true => Some(self.hub_table.clone()),
                            false => None,
                        };
                        let index_root = match request.is_hub {
                            true => match &self.store.index_root() {
                                Some(index) => Some(CidCbor::from(*index)),
                                _ => None,
                            },
                            false => None,
                        };

                        let peer_ma = match request.is_hub {
                            true => self.multiaddr.first().cloned(),
                            false => None,
                        };

                        let msg = DiscoveryResponse {
                            id: request.id,
                            hub_table,
                            index_root,
                            peer_ma,
                        };
                        self.inner.send_response(channel, msg).unwrap();
                    }
                    RequestResponseMessage::Response {
                        request_id: _,
                        response,
                    } => {
                        //  addresses only get returned on a successful response
                        //  update our local peer table
                        //  extend overwrites colliding keys (we assume inbound information is most up to date)
                        if let Some(table) = response.hub_table {
                            self.hub_table.extend(table);
                        }

                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            DiscoveryEvent::ResponseReceived(
                                response.id,
                                peer,
                                response.index_root,
                                response.peer_ma,
                            ),
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
    use blockstore::memory::MemoryDB;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{Multiaddr, PeerId, Swarm, Transport};
    use serde_cbor::{from_slice, to_vec};
    use std::net::Ipv4Addr;

    #[test]
    fn request_serialization() {
        let req = DiscoveryRequest {
            id: 1,
            is_hub: true,
        };

        let msgc = req.clone();
        let msg_encoded = to_vec(&req).unwrap();

        let des_msg = from_slice(&msg_encoded).unwrap();

        assert_eq!(msgc, des_msg)
    }

    #[test]
    fn response_serialization() {
        let localhost = Ipv4Addr::new(127, 0, 0, 1);
        let multiaddr = Vec::from([Multiaddr::from(localhost)]);
        let peer = PeerIdCbor {
            bytes: "bafybeifkmbvagt6iaoj5l5xc5ids72wati3ex2t65dipwgukdo73zibrbi"
                .as_bytes()
                .to_vec(),
        };
        let mut hub_table = HashMap::new();
        hub_table.insert(peer.clone(), multiaddr);
        let resp = DiscoveryResponse {
            // source: peer,
            id: 1,
            hub_table: Some(hub_table),
            index_root: None,
            peer_ma: Some(Multiaddr::from(localhost)),
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
        swarm: Swarm<Discovery<MemoryDB>>,
    }

    impl Peer {
        fn new(num_addreses: usize, is_hub: bool) -> Self {
            let (peer_id, trans) = mk_transport();
            let mut swarm = Swarm::new(
                trans,
                Discovery::new(
                    Config::default(),
                    peer_id,
                    is_hub,
                    Arc::new(MemoryDB::default()),
                ),
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

        fn get_hub_table(&mut self) -> PeerTable {
            return self.swarm.behaviour_mut().hub_table.clone();
        }

        fn add_address(&mut self, peer: &Peer) {
            for addr in peer.addr.clone() {
                self.swarm.behaviour_mut().add_address(&peer.peer_id, addr);
            }
        }

        fn swarm(&mut self) -> &mut Swarm<Discovery<MemoryDB>> {
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
        if let Some(DiscoveryEvent::ResponseReceived(resp_id, _, _, _)) = event {
            assert_eq!(resp_id, id);
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

        println!("before {:?}", peer3.get_hub_table());
        println!("before {:?}", peer1.get_hub_table());

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

        println!("after {:?}", peer3.get_hub_table());
        println!("after {:?}", peer1.get_hub_table());

        let table1 = peer1.get_hub_table();
        let lock1 = table1;

        let table3 = peer3.get_hub_table();
        let lock3 = table3;

        let mut k1: Vec<&PeerIdCbor> = lock1.keys().collect();
        let mut k3: Vec<&PeerIdCbor> = lock3.keys().collect();

        assert_eq!(k1.sort(), k3.sort());

        let mut v1: Vec<&Vec<Multiaddr>> = lock1.values().collect();
        let mut v3: Vec<&Vec<Multiaddr>> = lock3.values().collect();

        assert_eq!(v1.sort(), v3.sort());
    }
}
