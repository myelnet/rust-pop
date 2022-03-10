mod discovery;
mod routing;
mod utils;
pub use crate::discovery::Config as DiscoveryConfig;
pub use crate::discovery::{DiscoveryEvent, HubDiscovery, PeerTable, SerializablePeerTable};
pub use crate::routing::{
    RoutingNetEvent, RoutingNetwork, RoutingRecord, EMPTY_QUEUE_SHRINK_THRESHOLD,
};
pub use utils::{gossip_init, peer_table_from_bytes, peer_table_to_bytes};

use bimap::BiMap;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use libipld::codec::Decode;
use libipld::{Cid, Ipld};
use libp2p::gossipsub::{Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::{Multiaddr, NetworkBehaviour, PeerId};
use serde::{Deserialize, Serialize};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
pub type Index = HashMap<Cid, PeerTable>;
pub type LocalIndex = HashSet<Cid>;
pub type SerializableIndex = HashMap<Vec<u8>, SerializablePeerTable>;
pub type SerializableLocalIndex = HashSet<Vec<u8>>;
use std::str::FromStr;

pub const ROUTING_TOPIC: &str = "myel/content-routing";
pub const SYNCING_TOPIC: &str = "myel/index-syncing";

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum RoutingTableEntry {
    Insertion {
        multiaddresses: Vec<Multiaddr>,
        cids: Vec<CidCbor>,
    },
    Deletion {
        cids: Vec<CidCbor>,
    },
}
impl Cbor for RoutingTableEntry {}

#[derive(Debug, PartialEq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct ContentRequest {
    pub multiaddresses: Vec<Multiaddr>,
    pub root: CidCbor,
}
impl Cbor for ContentRequest {}

#[derive(Debug)]
pub enum RoutingEvent {
    ContentRequestBroadcast(String),
    ContentRequestFulfilled(String),
    FoundContent(String),
    SyncRequestBroadcast,
    HubTableUpdated(PeerId, Option<CidCbor>),
    HubIndexUpdated,
    RoutingTableUpdated(Cid),
}

#[derive(Debug)]
pub struct RoutingConfig {
    pub is_hub: bool,
    pub peer_id: PeerId,
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "RoutingEvent", poll_method = "poll", event_process = true)]
pub struct Routing {
    broadcaster: Gossipsub,
    pub discovery: HubDiscovery,
    routing_responder: RoutingNetwork,

    #[behaviour(ignore)]
    pub config: RoutingConfig,
    #[behaviour(ignore)]
    pending_events: VecDeque<RoutingEvent>,
    // a map of who has the content we made requests for
    #[behaviour(ignore)]
    pub routing_table: Arc<RwLock<Index>>,
    // A bidirectional map of index cids we have made data-transfer requests for.
    // This will always be empty if the node is not a hub
}

impl Routing {
    pub fn new(config: RoutingConfig, index_root: Option<CidCbor>) -> Self {
        let discovery = HubDiscovery::new(
            DiscoveryConfig::default(),
            config.peer_id,
            config.is_hub,
            index_root,
        );
        //  topic with identity hash
        let broadcaster = gossip_init(
            config.peer_id,
            Vec::from([
                IdentTopic::new(ROUTING_TOPIC),
                IdentTopic::new(SYNCING_TOPIC),
            ]),
        );

        Self {
            broadcaster,
            discovery,
            config,
            routing_responder: RoutingNetwork::new(),
            pending_events: VecDeque::default(),
            // can swap this for something on disk, this is a thread safe hashmap
            routing_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.discovery.add_address(peer_id, addr);
        self.broadcaster.add_explicit_peer(peer_id);
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.discovery.addresses_of_peer(peer_id)
    }

    pub fn publish_update(&mut self, msg: RoutingTableEntry) -> Result<(), String> {
        // Because Topic is not thread safe (the hasher it uses can't be safely shared across threads)
        // we create a new instantiation of the Topic for each publication, in most examples Topic is
        // cloned anyway
        self.broadcaster
            .publish(
                IdentTopic::new(SYNCING_TOPIC),
                msg.marshal_cbor().map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
        self.pending_events
            .push_back(RoutingEvent::SyncRequestBroadcast);

        Ok(())
    }

    pub fn publish_insertion(&mut self, cids: Vec<CidCbor>) -> Result<(), String> {
        let msg = RoutingTableEntry::Insertion {
            multiaddresses: self.discovery.multiaddr.to_vec(),
            cids,
        };
        self.publish_update(msg)
    }
    pub fn publish_deletion(&mut self, cids: Vec<CidCbor>) -> Result<(), String> {
        let msg = RoutingTableEntry::Deletion { cids };
        self.publish_update(msg)
    }

    pub fn find_content(&mut self, root: Cid) -> Result<(), String> {
        //  we have an entry in our routing table
        if self.routing_table.read().unwrap().contains_key(&root) {
            return Ok(());
        }

        let msg = ContentRequest {
            multiaddresses: self.discovery.multiaddr.to_vec(),
            root: CidCbor::from(root),
        }
        .marshal_cbor()
        .map_err(|e| e.to_string())?;

        // Because Topic is not thread safe (the hasher it uses can't be safely shared across threads)
        // we create a new instantiation of the Topic for each publication, in most examples Topic is
        // cloned anyway
        self.broadcaster
            .publish(IdentTopic::new(ROUTING_TOPIC), msg)
            .map_err(|e| e.to_string())?;
        self.pending_events
            .push_back(RoutingEvent::ContentRequestBroadcast(root.to_string()));

        Ok(())
    }

    pub fn insert_routing_entry(
        &mut self,
        peer_id: PeerId,
        entry: RoutingTableEntry,
    ) -> Result<(), String> {
        match entry {
            RoutingTableEntry::Insertion {
                multiaddresses: ma,
                cids,
            } => {
                let mut addr_vec = SmallVec::<[Multiaddr; 4]>::new();
                // check associated multiaddresses are valid
                for addr in ma {
                    addr_vec.push(addr)
                }
                let peer_table: PeerTable = HashMap::from([(peer_id, addr_vec)]);
                let mut lock = self.routing_table.write().unwrap();
                for cid in cids {
                    // check sent cids iare valid
                    if let Some(c) = cid.to_cid() {
                        lock.insert(c, peer_table.clone());
                    }
                    // quit if a single CID is invalid, this can be relaxed
                    else {
                        return Err("contained an invalid cid".to_string());
                    }
                }
            }
            RoutingTableEntry::Deletion { cids } => {
                for cid in cids {
                    // check sent cids are valid
                    let mut lock = self.routing_table.write().unwrap();
                    if let Some(c) = cid.to_cid() {
                        if let Some(peer_table) = lock.get_mut(&c) {
                            peer_table.remove(&peer_id);
                            // if empty clear the entry for the CID
                            if peer_table.is_empty() {
                                lock.remove(&c);
                            }
                        }
                    }
                    // quit if a single CID is invalid, this can be relaxed
                    else {
                        return Err("contained an invalid cid".to_string());
                    }
                }
            }
        }
        Ok(())
    }

    fn process_routing_response(&mut self, peer_table: PeerTable, root: Cid) {
        // expand table as new entries come in
        let mut new_entry = HashMap::new();
        new_entry.insert(root, peer_table);
        self.routing_table.write().unwrap().extend(new_entry);
        self.pending_events
            .push_back(RoutingEvent::RoutingTableUpdated(root));
    }

    fn process_sync_request(
        &mut self,
        peer_id: PeerId,
        message: GossipsubMessage,
    ) -> Result<(), String> {
        // only hubs should respond
        if self.config.is_hub {
            let entry =
                RoutingTableEntry::unmarshal_cbor(&message.data).map_err(|e| e.to_string())?;
            self.insert_routing_entry(peer_id, entry).map_err(|e| e)?;
            self.pending_events.push_back(RoutingEvent::HubIndexUpdated);
        }

        Ok(())
    }

    fn process_routing_request(
        &mut self,
        peer_id: PeerId,
        message: GossipsubMessage,
    ) -> Result<(), String> {
        // only hubs should respond
        if self.config.is_hub {
            let req = ContentRequest::unmarshal_cbor(&message.data).map_err(|e| e.to_string())?;
            //  if the sent Cid is valid
            if let Some(r) = req.root.to_cid() {
                // if !self.store.contains(&r).map_err(|e| e.to_string())? {
                //  if routing table contains the root cid, if not then do nothing
                if let Some(peer_table) = self.routing_table.read().unwrap().get(&r) {
                    let message = RoutingRecord {
                        root: req.root,
                        peers: Some(peer_table_to_bytes(peer_table)),
                    };
                    self.routing_responder.send_message(&peer_id, message);
                    self.pending_events
                        .push_back(RoutingEvent::FoundContent(r.to_string()));
                }
                // }
            } else {
                return Err("invalid cid".to_string());
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
            DiscoveryEvent::ResponseReceived(_, peer, index) => self
                .pending_events
                .push_back(RoutingEvent::HubTableUpdated(peer, index)),
        }
    }
}

impl NetworkBehaviourEventProcess<RoutingNetEvent> for Routing {
    fn inject_event(&mut self, event: RoutingNetEvent) {
        match event {
            RoutingNetEvent::Response(_, resp) => {
                if let Some(peers) = resp.peers {
                    if let Some(r) = resp.root.to_cid() {
                        self.process_routing_response(peer_table_from_bytes(&peers), r)
                    }
                }
            }
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
                if let Some(s) = message.source {
                    if message.topic == IdentTopic::new(ROUTING_TOPIC).hash() {
                        self.process_routing_request(s, message).unwrap();
                    } else if message.topic == IdentTopic::new(SYNCING_TOPIC).hash() {
                        self.process_sync_request(s, message).unwrap();
                    }
                }
            }
            _ => println!("gossip event: {:?}", event),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use futures::prelude::*;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use rand::prelude::*;
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
        bs: Arc<MemoryBlockStore>,
    }

    impl Peer {
        fn new(is_hub: bool) -> Self {
            let (peer_id, trans) = mk_transport();
            let config = RoutingConfig { peer_id, is_hub };
            let rt = Routing::new(config, None);
            let mut swarm = Swarm::new(trans, rt, peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
                bs: Arc::new(MemoryBlockStore::default()),
            }
        }

        fn get_routing_table(&mut self) -> Arc<RwLock<Index>> {
            return self.swarm.behaviour_mut().routing_table.clone();
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn request_content(&mut self, content: Cid) {
            self.swarm().behaviour_mut().find_content(content).unwrap();
        }

        fn make_routing_table(&mut self, peer: PeerId) -> RoutingTableEntry {
            // generate 1 random byte
            const FILE_SIZE: usize = 1;
            let mut data = vec![0u8; FILE_SIZE];
            rand::thread_rng().fill_bytes(&mut data);

            let root = dag_service::add(self.bs.clone(), &data).unwrap().unwrap();

            let entry = RoutingTableEntry::Insertion {
                multiaddresses: Vec::from([self.addr.clone()]),
                cids: Vec::from([CidCbor::from(root)]),
            };

            self.swarm()
                .behaviour_mut()
                .insert_routing_entry(peer, entry.clone())
                .unwrap();

            entry
        }

        fn push_update(&mut self, content: Vec<&str>, deletion: bool) -> Vec<CidCbor> {
            let mut cids = Vec::new();
            for cid in content {
                cids.push(CidCbor::from(Cid::try_from(cid).unwrap()));
            }
            if deletion {
                self.swarm()
                    .behaviour_mut()
                    .publish_deletion(cids.clone())
                    .unwrap();
            } else {
                self.swarm()
                    .behaviour_mut()
                    .publish_insertion(cids.clone())
                    .unwrap();
            }
            cids
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

        async fn next_discovery(&mut self) -> Option<RoutingEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        RoutingEvent::HubTableUpdated(..) => return Some(event),
                        _ => println!("{:?}", event),
                    }
                }
            }
        }

        async fn next_routing(&mut self) -> Option<RoutingEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        RoutingEvent::RoutingTableUpdated(_) => return Some(event),
                        _ => {}
                    }
                }
            }
        }

        async fn next_sync(&mut self) -> Option<RoutingEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        RoutingEvent::SyncRequestBroadcast => return Some(event),
                        _ => println!("{:?}", event),
                    }
                }
            }
        }
    }

    #[async_std::test]
    async fn test_can_update_index() {
        //  will have empty hub table as not a hub
        let mut peer1 = Peer::new(false);

        // will have themselves in hub table
        let mut peer2 = Peer::new(true);

        peer1.add_address(&peer2);
        //
        // //  print logs for peer 2
        let hub_routing_table = peer2.get_routing_table().clone();
        let peer2id = peer2.spawn("peer2");

        std::thread::sleep(std::time::Duration::from_millis(1000));
        //
        // peer3.swarm().dial(peer2id).unwrap();
        peer1.swarm().dial(peer2id).unwrap();

        peer1.next_discovery().await;

        let cids = peer1.push_update(
            Vec::from(["bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq"]),
            false,
        );

        peer1.next_sync().await;

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let cid = &cids.first().unwrap().to_cid().unwrap();

        assert!(hub_routing_table.read().unwrap().contains_key(cid));

        assert!(hub_routing_table
            .read()
            .unwrap()
            .get(cid)
            .unwrap()
            .contains_key(&peer1.peer_id));

        assert_eq!(
            hub_routing_table
                .read()
                .unwrap()
                .get(cid)
                .unwrap()
                .get(&peer1.peer_id)
                .unwrap()
                .clone(),
            SmallVec::<[Multiaddr; 6]>::from(
                peer1.swarm().behaviour_mut().discovery.multiaddr.to_vec()
            )
        );

        peer1.push_update(
            Vec::from(["bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq"]),
            true,
        );

        peer1.next_sync().await;

        std::thread::sleep(std::time::Duration::from_millis(1000));

        assert!(hub_routing_table.read().unwrap().is_empty())
    }

    #[async_std::test]
    async fn test_routing() {
        let mut hub_peer = Peer::new(true);
        let entry = hub_peer.make_routing_table(hub_peer.peer_id);
        let cid = match entry {
            RoutingTableEntry::Insertion {
                multiaddresses: _,
                cids,
            } => Some(cids.first().unwrap().to_cid().unwrap()),
            _ => None,
        };

        let mut peer1 = Peer::new(false);

        peer1.add_address(&hub_peer);

        //  print logs for hub peer
        let hub_table = hub_peer.get_routing_table().clone();
        let hubid = hub_peer.spawn("hub");

        //  print logs for hub peer
        peer1.swarm().dial(hubid).unwrap();
        peer1.next_discovery().await;

        //  print logs for hub peer
        println!("discovery done");

        peer1.request_content(cid.unwrap());

        peer1.next_routing().await;

        let lock1 = hub_table.read().unwrap();

        let table1 = peer1.get_routing_table();
        let lock2 = table1.read().unwrap();

        let mut k1: Vec<&Cid> = lock1.keys().collect();
        let mut k2: Vec<&Cid> = lock2.keys().collect();

        assert_eq!(k1.sort(), k2.sort());

        let v1: Vec<&HashMap<PeerId, SmallVec<[Multiaddr; 4]>>> = lock1.values().collect();
        let v3: Vec<&HashMap<PeerId, SmallVec<[Multiaddr; 4]>>> = lock2.values().collect();

        assert_eq!(v1, v3);
    }
}
