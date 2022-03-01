mod hub_discovery;
mod hub_indexing;
mod network;
mod utils;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use hub_discovery::Config as DiscoveryConfig;
use hub_discovery::{DiscoveryEvent, HubDiscovery, PeerTable};
use hub_indexing::Config as IndexingConfig;
use hub_indexing::{ContentTable, HubIndexing, IndexingEvent};
use libp2p::gossipsub::{Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::{Multiaddr, NetworkBehaviour, PeerId};
use network::{PeersforContent, RoutingNetEvent, RoutingNetwork, EMPTY_QUEUE_SHRINK_THRESHOLD};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use utils::content_routing_init;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ContentRequest {
    pub multiaddresses: Vec<Multiaddr>,
    pub root: CidCbor,
}
impl Cbor for ContentRequest {}

#[derive(Debug)]
pub enum RoutingEvent {
    RequestSent(Vec<u8>),
    FoundContent(Vec<u8>),
    HubTableUpdated,
    HubIndexUpdated,
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "RoutingEvent", poll_method = "poll", event_process = true)]
pub struct Routing {
    hub_discovery: HubDiscovery,
    hub_indexing: HubIndexing,
    content_routing: Gossipsub,
    routing_network: RoutingNetwork,

    #[behaviour(ignore)]
    peer_id: PeerId,
    #[behaviour(ignore)]
    is_hub: bool,
    #[behaviour(ignore)]
    pending_events: VecDeque<RoutingEvent>,
    #[behaviour(ignore)]
    multiaddresses: Arc<Vec<Multiaddr>>,
    #[behaviour(ignore)]
    routing_table: ContentTable,
}

impl Routing {
    pub fn new(peer_id: PeerId, is_hub: bool, hub_table: Option<Arc<RwLock<PeerTable>>>) -> Self {
        // if no hub table was passed then initialize a dummy one (mainly for testing purposes)
        let hub_table: Arc<RwLock<PeerTable>> =
            hub_table.unwrap_or_else(|| Arc::new(RwLock::new(HashMap::new())));
        let hub_discovery = HubDiscovery::new(
            DiscoveryConfig::default(),
            peer_id,
            is_hub,
            Some(hub_table.clone()),
        );
        let hub_indexing = HubIndexing::new(IndexingConfig::default(), is_hub, Some(hub_table));
        //  topic with identity hash
        let content_routing =
            content_routing_init(peer_id, IdentTopic::new("myel/content-routing"));

        Self {
            peer_id,
            is_hub,
            hub_discovery,
            hub_indexing,
            content_routing,
            routing_network: RoutingNetwork::new(),
            pending_events: VecDeque::default(),
            multiaddresses: Arc::new(Vec::new()),
            routing_table: HashMap::new(),
        }
    }

    pub fn update_hubs(&mut self) {
        self.hub_indexing.update();
    }

    pub fn find_content(&mut self, root: CidCbor) -> Result<(), io::Error> {
        let msg = ContentRequest {
            multiaddresses: self.multiaddresses.to_vec(),
            root,
        }
        .marshal_cbor()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        // Because Topic is not thread safe (the hasher it uses can't be safely shared across threads)
        // we create a new instantiation of the Topic for each publication, in most examples Topic is
        // cloned anyway
        let msg_id = self
            .content_routing
            .publish(IdentTopic::new("myel/content-routing"), msg)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        self.pending_events
            .push_back(RoutingEvent::RequestSent(msg_id.0));
        Ok(())
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.hub_discovery.add_address(peer_id, addr);
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.hub_discovery.addresses_of_peer(peer_id)
    }

    fn process_discovery_response(&mut self) {
        self.pending_events.push_back(RoutingEvent::HubTableUpdated);
    }

    fn process_indexing_response(&mut self) {
        self.pending_events.push_back(RoutingEvent::HubIndexUpdated);
    }

    fn delete_routing_entry(&mut self, root: &CidCbor) {
        self.routing_table.remove(root);
    }

    fn reset_routing_table(&mut self) {
        self.routing_table = HashMap::new();
    }

    fn process_routing_response(&mut self, root: CidCbor, peer_table: PeerTable) {
        let mut new_entry = HashMap::new();
        new_entry.insert(root, peer_table);
        // expand table as new entries come in -- allows for async updates
        self.routing_table.extend(new_entry);
        self.pending_events.push_back(RoutingEvent::HubIndexUpdated);
    }

    fn process_gossip_message(
        &mut self,
        peer_id: PeerId,
        message: GossipsubMessage,
    ) -> Result<(), io::Error> {
        // only hubs should respond
        if self.is_hub {
            let req = ContentRequest::unmarshal_cbor(&message.data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

            //  if the sent Cid is valid
            if let Some(r) = req.root.to_cid() {
                //  if content table contains the root cid, if not then do nothing
                let table_read = self.hub_indexing.content_table.read().unwrap();
                if let Some(peer_table) = table_read.get(&req.root) {
                    let message = PeersforContent {
                        root: req.root,
                        peers: Some(utils::peer_table_to_bytes(peer_table)),
                    };
                    self.routing_network.send_message(&peer_id, message);
                    self.pending_events
                        .push_back(RoutingEvent::FoundContent(r.to_bytes()));
                }
            } else {
                return Err(io::Error::new(io::ErrorKind::Other, "invalid cid"));
            }
        }

        Ok(())
    }

    fn poll(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self as NetworkBehaviour>::OutEvent,
            <Self as NetworkBehaviour>::ProtocolsHandler,
        >,
    > {
        if let Some(ev) = self.pending_events.pop_front() {
            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(ev));
        } else if self.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.pending_events.shrink_to_fit();
        }
        Poll::Pending
    }
}

impl NetworkBehaviourEventProcess<DiscoveryEvent> for Routing {
    fn inject_event(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::ResponseReceived(_) => self.process_discovery_response(),
        }
    }
}

impl NetworkBehaviourEventProcess<RoutingNetEvent> for Routing {
    fn inject_event(&mut self, event: RoutingNetEvent) {
        match event {
            RoutingNetEvent::Response(_, resp) => {
                if let Some(peers) = resp.peers {
                    self.process_routing_response(resp.root, utils::peer_table_from_bytes(&peers))
                }
            }
        }
    }
}

impl NetworkBehaviourEventProcess<IndexingEvent> for Routing {
    fn inject_event(&mut self, event: IndexingEvent) {
        match event {
            IndexingEvent::ResponseReceived(_) => self.process_indexing_response(),
        }
    }
}

impl NetworkBehaviourEventProcess<GossipsubEvent> for Routing {
    fn inject_event(&mut self, event: GossipsubEvent) {
        match event {
            GossipsubEvent::Message {
                propagation_source: peer_id,
                message_id: id,
                message,
            } => {
                println!(
                    "Got message: {} with id: {} from peer: {:?}",
                    String::from_utf8_lossy(&message.data),
                    id,
                    peer_id
                );
                self.process_gossip_message(peer_id, message);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use dag_service::add;
    use futures::prelude::*;
    use libipld::Cid;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use smallvec::SmallVec;
    use std::time::Duration;

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
        swarm: Swarm<Routing>,
    }

    impl Peer {
        fn new(is_hub: bool, hub_table: Option<Arc<RwLock<PeerTable>>>) -> Self {
            let (peer_id, trans) = mk_transport();
            let rt = Routing::new(peer_id, is_hub, hub_table);
            let mut swarm = Swarm::new(trans, rt, peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
            }
        }

        fn get_hub_table(&mut self) -> Arc<RwLock<PeerTable>> {
            return self.swarm.behaviour_mut().hub_discovery.hub_table.clone();
        }

        fn get_content_table(&mut self) -> Arc<RwLock<ContentTable>> {
            return self
                .swarm
                .behaviour_mut()
                .hub_indexing
                .content_table
                .clone();
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn swarm(&mut self) -> &mut Swarm<Routing> {
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

        async fn next(&mut self) -> Option<RoutingEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        RoutingEvent::HubTableUpdated => {}
                        RoutingEvent::HubIndexUpdated => {}
                        _ => return Some(event),
                    }
                }
            }
        }

        async fn next_discovery(&mut self) -> Option<RoutingEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        RoutingEvent::HubTableUpdated => return Some(event),
                        _ => {}
                    }
                }
            }
        }

        async fn next_indexing(&mut self) -> Option<RoutingEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        RoutingEvent::HubIndexUpdated => return Some(event),
                        _ => {}
                    }
                }
            }
        }
    }

    #[async_std::test]
    async fn test_peer_discovery() {
        //  will have empty hub table as not a hub
        let mut peer1 = Peer::new(false, None);
        // will have themselves in hub table
        let peer2 = Peer::new(true, None);
        let mut peer3 = Peer::new(true, None);

        // peer 3 knows peer 2 only and itself
        peer3.add_address(&peer2);
        // peer 1 knows peer 3 only and itself
        peer1.add_address(&peer2);

        //  print logs for peer 2
        let peer2id = peer2.spawn("peer2");

        peer3.swarm().dial(peer2id).unwrap();
        peer1.swarm().dial(peer2id).unwrap();

        peer3.next_discovery().await;
        peer1.next_discovery().await;

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

    #[async_std::test]
    async fn test_index_update() {
        let cid = Cid::try_from("bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq")
            .unwrap();
        let content = CidCbor::from(cid);
        let host_peer = Peer::new(true, None);
        let multiaddr = SmallVec::<[Multiaddr; 6]>::from(Vec::from([host_peer.addr]));
        let peer_id = host_peer.peer_id;
        let peer_table: PeerTable = HashMap::from([(peer_id, multiaddr)]);

        let mut hub_peer = Peer::new(true, None);
        let hub_id = hub_peer.peer_id;
        let hub_addr = SmallVec::<[Multiaddr; 6]>::from(Vec::from([hub_peer.addr.clone()]));
        let hub_table: PeerTable = HashMap::from([(hub_id, hub_addr)]);
        let mut peer1 = Peer::new(false, Some(Arc::new(RwLock::new(hub_table))));
        peer1
            .swarm()
            .behaviour_mut()
            .hub_indexing
            .add_entry(content, peer_table);

        let mut peer2 = Peer::new(false, None);

        peer1.add_address(&hub_peer);
        peer1.add_address(&peer2);

        //  print logs for peer 2
        let hub_table = hub_peer.get_content_table().clone();
        hub_peer.spawn("hub");

        // update remote hubs
        peer1.swarm().behaviour_mut().update_hubs();
        peer1.next_indexing().await;

        // assert peer 2 table did not update
        assert!(peer2.get_content_table().read().unwrap().is_empty());

        assert_eq!(
            *hub_table.read().unwrap(),
            *peer1.get_content_table().read().unwrap()
        );
    }

}
