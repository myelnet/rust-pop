use crate::hub_discovery::{PeerTable, SerializablePeerTable};
use crate::utils::{content_table_from_bytes, content_table_to_bytes};
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
use std::collections::HashMap;
use std::error::Error;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, RwLock,
};
use std::time::Duration;

pub type RequestId = i32;
pub type ContentTable = HashMap<CidCbor, PeerTable>;
pub type SerializableContentTable = HashMap<Vec<u8>, SerializablePeerTable>;

#[derive(Debug, Clone)]
pub struct HubIndexingProtocol;

impl ProtocolName for HubIndexingProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/myel/hub-indexing/0.1"
    }
}

#[derive(Clone)]
pub struct IndexingCodec {
    max_msg_size: usize,
}

impl Default for IndexingCodec {
    fn default() -> Self {
        Self {
            max_msg_size: 1000000,
        }
    }
}

#[async_trait]
impl RequestResponseCodec for IndexingCodec {
    type Protocol = HubIndexingProtocol;
    type Response = IndexingResponse;
    type Request = IndexingRequest;

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
pub enum IndexingEvent {
    ResponseReceived(IndexingResponse),
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
pub struct IndexingRequest {
    pub id: RequestId,
    pub content_table: SerializableContentTable,
}

#[derive(Debug, PartialEq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct IndexingResponse {
    pub id: RequestId,
}

pub struct HubIndexing {
    //  we implement our own id_counter (instead of libp2p's) to ensure the request / response messages are CBOR encodable
    id_counter: Arc<AtomicI32>,
    inner: RequestResponse<IndexingCodec>,
    pub content_table: Arc<RwLock<ContentTable>>,
    hub_table: Arc<RwLock<PeerTable>>,
}

impl HubIndexing {
    pub fn new(config: Config, hub: bool, hub_table: Option<Arc<RwLock<PeerTable>>>) -> Self {
        // if node is a hub can support inbound and outbound requests -- i.e can sync with other hubs and receive
        //  updates from other nodes
        let support = if hub {
            ProtocolSupport::Full
        // if node is not a hub then we can only make outbound requests and reject inbound requests
        } else {
            ProtocolSupport::Outbound
        };
        let protocols = std::iter::once((HubIndexingProtocol, support));
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let inner = RequestResponse::new(IndexingCodec::default(), protocols, rr_config);
        let content_table = Arc::new(RwLock::new(HashMap::new()));

        // if no hub table was passed then initialize a dummy one (mainly for testing purposes)
        let hub_table: Arc<RwLock<PeerTable>> =
            hub_table.unwrap_or_else(|| Arc::new(RwLock::new(HashMap::new())));

        Self {
            id_counter: Arc::new(AtomicI32::new(1)),
            inner,
            hub_table,
            content_table,
        }
    }

    pub fn add_entry(&mut self, cid: CidCbor, peer_table: PeerTable) {
        self.content_table.write().unwrap().insert(cid, peer_table);
    }

    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.inner.add_address(peer, address);
    }

    /// Removes an address of a peer previously added via `add_address`.
    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        self.inner.remove_address(peer, address);
    }

    pub fn update(&mut self) -> Vec<RequestId> {
        // send content table to all hubs -- currently is very naive and simply sends the whole table
        // TODO: only send additions and deletions
        let mut req_ids = Vec::new();
        for peer_id in self.hub_table.read().unwrap().keys() {
            let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
            let content = content_table_to_bytes(&self.content_table.read().unwrap());
            self.inner.send_request(
                peer_id,
                IndexingRequest {
                    id,
                    content_table: content,
                },
            );
            req_ids.push(id);
        }
        req_ids
    }
}

impl NetworkBehaviour for HubIndexing {
    type ProtocolsHandler = <RequestResponse<IndexingCodec> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = IndexingEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.inner.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
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
        // include self in peer table for syncing
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
                        let new_content = content_table_from_bytes(&request.clone().content_table);
                        //  update our local content table,
                        //  extend overwrites colliding keys (we assume inbound information is most up to date)
                        self.content_table.write().unwrap().extend(new_content);
                        let msg = IndexingResponse { id: request.id };
                        // send response so we know the request was successful -- if we don't receive a response
                        // we can prune hubs from our peer table etc...
                        self.inner.send_response(channel, msg).unwrap();
                    }
                    RequestResponseMessage::Response {
                        request_id: _,
                        response,
                    } => {
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            IndexingEvent::ResponseReceived(response),
                        ));
                    }
                },
                RequestResponseEvent::OutboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    println!(
                        "peer Indexing outbound failure {} {} {:?}",
                        peer, request_id, error
                    );
                }
                RequestResponseEvent::InboundFailure {
                    peer,
                    request_id,
                    error,
                } => {
                    println!(
                        "peer Indexing inbound failure {} {} {:?}",
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
    use libipld::Cid;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{Multiaddr, PeerId, Swarm, Transport};
    use serde_cbor::{from_slice, to_vec};
    use smallvec::SmallVec;

    #[test]
    fn request_serialization() {
        let content = "12D3KooWET6WqHULAHLXVLnid8gFzyqEhKRQupt5hMWuqw9gwv2s"
            .as_bytes()
            .to_vec();
        let multiaddr = Vec::from(["/ip4/127.0.0.1/tcp/0".as_bytes().to_vec()]);
        let peer = "/ip4/127.0.0.1/tcp/0".as_bytes().to_vec();
        let mut addresses = HashMap::new();
        addresses.insert(peer, multiaddr);
        let mut content_table = HashMap::new();
        content_table.insert(content, addresses);

        let req = IndexingRequest {
            id: 1,
            content_table,
        };

        let msgc = req.clone();
        let msg_encoded = to_vec(&req).unwrap();

        let des_msg = from_slice(&msg_encoded).unwrap();

        assert_eq!(msgc, des_msg)
    }

    #[test]
    fn response_serialization() {
        let resp = IndexingResponse { id: 1 };

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
        swarm: Swarm<HubIndexing>,
    }

    impl Peer {
        fn new(
            num_addreses: usize,
            is_hub: bool,
            hub_table: Option<Arc<RwLock<PeerTable>>>,
        ) -> Self {
            let (peer_id, trans) = mk_transport();
            let mut swarm = Swarm::new(
                trans,
                HubIndexing::new(Config::default(), is_hub, hub_table),
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

        fn add_address(&mut self, peer: &Peer) {
            for addr in peer.addr.clone() {
                self.swarm.behaviour_mut().add_address(&peer.peer_id, addr);
            }
        }

        fn swarm(&mut self) -> &mut Swarm<HubIndexing> {
            &mut self.swarm
        }

        fn spawn(mut self, _name: &'static str) -> PeerId {
            let peer_id = self.peer_id;
            task::spawn(async move {
                loop {
                    let _event = self.swarm.next().await;
                }
            });
            peer_id
        }

        async fn next(&mut self) -> Option<IndexingEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    return Some(event);
                }
            }
        }
    }

    fn assert_response_ok(event: Option<IndexingEvent>, id: RequestId) {
        if let Some(IndexingEvent::ResponseReceived(responses)) = event {
            assert_eq!(responses.id, id);
        } else {
            panic!("{:?} is not a response event", event);
        }
    }

    #[async_std::test]
    async fn test_index_update() {
        let cid = Cid::try_from("bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq")
            .unwrap();
        let content = CidCbor::from(cid);
        let host_peer = Peer::new(1, true, None);
        let multiaddr = SmallVec::<[Multiaddr; 6]>::from(host_peer.addr);
        let peer_id = host_peer.peer_id;
        let peer_table: PeerTable = HashMap::from([(peer_id, multiaddr)]);

        let mut hub_peer = Peer::new(3, true, None);
        let hub_id = hub_peer.peer_id;
        let hub_addr = SmallVec::<[Multiaddr; 6]>::from(hub_peer.addr.clone());
        let hub_table: PeerTable = HashMap::from([(hub_id, hub_addr)]);
        let mut peer1 = Peer::new(7, false, Some(Arc::new(RwLock::new(hub_table))));
        peer1.swarm().behaviour_mut().add_entry(content, peer_table);

        let mut peer2 = Peer::new(3, false, None);

        println!(
            "before {:?}",
            peer1.swarm().behaviour().content_table.read().unwrap()
        );
        println!(
            "before {:?}",
            peer2.swarm().behaviour().content_table.read().unwrap()
        );

        peer1.add_address(&hub_peer);
        peer1.add_address(&peer2);

        //  print logs for peer 2
        let hub_table = hub_peer.swarm().behaviour().content_table.clone();
        hub_peer.spawn("hub");

        // update remote hubs
        peer1.swarm().behaviour_mut().update();

        println!(
            "after {:?}",
            peer2.swarm().behaviour().content_table.read().unwrap()
        );
        println!(
            "after {:?}",
            peer1.swarm().behaviour().content_table.read().unwrap()
        );

        assert_response_ok(peer1.next().await, 1);

        // assert peer 2 table did not update
        assert!(peer2
            .swarm()
            .behaviour_mut()
            .content_table
            .read()
            .unwrap()
            .is_empty());

        assert_eq!(
            *hub_table.read().unwrap(),
            *peer1.swarm().behaviour().content_table.read().unwrap()
        );
    }
}
