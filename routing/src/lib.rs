mod cbor_helpers;
mod discovery;
pub mod index;
mod routing;
mod utils;
pub use crate::routing::{
    RoutingNetEvent, RoutingNetwork, RoutingRecord, EMPTY_QUEUE_SHRINK_THRESHOLD,
};
use blockstore::types::BlockStore;
pub use cbor_helpers::PeerIdCbor;
pub use discovery::Config as DiscoveryConfig;
pub use discovery::{DiscoveryEvent, HubDiscovery, PeerTable};
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use libipld::Cid;
use libp2p::gossipsub::{Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::{Multiaddr, NetworkBehaviour, PeerId};
use serde::{Deserialize, Serialize};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
pub use utils::gossip_init;
pub type LocalIndex<B> = index::IndexableBlockstore<B>;
pub type Index<B> = Arc<Mutex<index::Hamt<B, PeerTable>>>;

pub const ROUTING_TOPIC: &str = "myel/content-routing";
pub const INDEX_TOPIC: &str = "myel/index-updates";

pub fn tcid(v: Cid) -> index::BytesKey {
    index::BytesKey(v.to_bytes())
}

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
pub struct Routing<B: 'static + BlockStore> {
    broadcaster: Gossipsub,
    pub discovery: HubDiscovery<B>,
    routing_responder: RoutingNetwork,

    #[behaviour(ignore)]
    pub config: RoutingConfig,
    #[behaviour(ignore)]
    pending_events: VecDeque<RoutingEvent>,
    // a map of who has the content we made requests for
    #[behaviour(ignore)]
    pub routing_table: Index<B>,
}

impl<B: 'static + BlockStore> Routing<B> {
    pub fn new(config: RoutingConfig, store: Arc<B>) -> Self {
        let discovery = HubDiscovery::new(
            DiscoveryConfig::default(),
            config.peer_id,
            config.is_hub,
            store.clone(),
        );
        //  topic with identity hash
        let broadcaster = gossip_init(
            config.peer_id,
            Vec::from([IdentTopic::new(ROUTING_TOPIC), IdentTopic::new(INDEX_TOPIC)]),
        );

        Self {
            broadcaster,
            discovery,
            config,
            routing_responder: RoutingNetwork::new(),
            pending_events: VecDeque::default(),
            // can swap this for something on disk
            routing_table: Arc::new(Mutex::new(index::Hamt::new(store))),
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
                IdentTopic::new(INDEX_TOPIC),
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
        if self
            .routing_table
            .lock()
            .unwrap()
            .contains_key(&tcid(root))
            .map_err(|e| e.to_string())?
        {
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
                let mut addr_vec = Vec::<Multiaddr>::new();
                for addr in ma {
                    addr_vec.push(addr)
                }
                let peer_table: PeerTable = HashMap::from([(PeerIdCbor::from(peer_id), addr_vec)]);
                for cid in cids {
                    // check sent cids iare valid
                    let mut lock = self.routing_table.lock().unwrap();
                    if let Some(c) = cid.to_cid() {
                        lock.extend(tcid(c), peer_table.clone())
                            .map_err(|e| e.to_string())?;
                        lock.flush().map_err(|e| e.to_string())?;
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
                    if let Some(c) = cid.to_cid() {
                        let mut lock = self.routing_table.lock().unwrap();
                        lock.delete_subvalue(&tcid(c), PeerIdCbor::from(peer_id))
                            .map_err(|e| e.to_string())?;
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

    fn process_routing_response(&mut self, peer_table: PeerTable, root: Cid) -> Result<(), String> {
        // expand table as new entries come i
        let mut lock = self.routing_table.lock().unwrap();
        lock.extend(tcid(root), peer_table)
            .map_err(|e| e.to_string())?;
        lock.flush().map_err(|e| e.to_string())?;
        self.pending_events
            .push_back(RoutingEvent::RoutingTableUpdated(root));
        Ok(())
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
                //  if routing table contains the root cid, if not then do nothing
                if let Some(peer_table) = self
                    .routing_table
                    .lock()
                    .unwrap()
                    .get(&tcid(r))
                    .map_err(|e| e.to_string())?
                {
                    let message = RoutingRecord {
                        root: req.root,
                        peers: Some(peer_table.clone()),
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

impl<B: 'static + BlockStore> NetworkBehaviourEventProcess<DiscoveryEvent> for Routing<B> {
    fn inject_event(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::ResponseReceived(_, peer, index) => self
                .pending_events
                .push_back(RoutingEvent::HubTableUpdated(peer, index)),
        }
    }
}

impl<B: 'static + BlockStore> NetworkBehaviourEventProcess<RoutingNetEvent> for Routing<B> {
    fn inject_event(&mut self, event: RoutingNetEvent) {
        match event {
            RoutingNetEvent::Response(_, resp) => {
                if let Some(peers) = resp.peers {
                    if let Some(r) = resp.root.to_cid() {
                        match self.process_routing_response(peers, r) {
                            Ok(_) => {}
                            Err(e) => println!("failed to process routing response: {}", e),
                        }
                    }
                }
            }
        }
    }
}

impl<B: 'static + BlockStore> NetworkBehaviourEventProcess<GossipsubEvent> for Routing<B> {
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
                    } else if message.topic == IdentTopic::new(INDEX_TOPIC).hash() {
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
        swarm: Swarm<Routing<LocalIndex<MemoryBlockStore>>>,
        bs: Arc<LocalIndex<MemoryBlockStore>>,
    }

    impl Peer {
        fn new(is_hub: bool) -> Self {
            let (peer_id, trans) = mk_transport();
            let config = RoutingConfig { peer_id, is_hub };
            let store = Arc::new(MemoryBlockStore::default());
            let store = Arc::new(crate::index::IndexableBlockstore::new(store));
            let rt = Routing::new(config, store.clone());
            let mut swarm = Swarm::new(trans, rt, peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
                bs: store,
            }
        }

        fn get_routing_table(&self) -> Index<LocalIndex<MemoryBlockStore>> {
            return self.swarm.behaviour().routing_table.clone();
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

        fn swarm(&mut self) -> &mut Swarm<Routing<LocalIndex<MemoryBlockStore>>> {
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
        //  will have empty hub table as not a hub
        let mut peer2 = Peer::new(false);

        // will have themselves in hub table
        let hub = Peer::new(true);

        peer1.add_address(&hub);
        peer2.add_address(&hub);
        //
        // //  print logs for peer 2
        let hub_routing_table = hub.get_routing_table();
        println!("before {:?}", hub_routing_table);
        let hubid = hub.spawn("hub");

        //
        peer2.swarm().dial(hubid).unwrap();
        peer1.swarm().dial(hubid).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5000));

        peer1.next_discovery().await;
        peer2.next_discovery().await;

        let cids = peer1.push_update(
            Vec::from(["bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq"]),
            false,
        );

        peer2.push_update(
            Vec::from(["bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq"]),
            false,
        );

        peer1.next_sync().await;

        peer2.next_sync().await;

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let cid = &tcid(cids.first().unwrap().to_cid().unwrap());

        println!("hub after {:?}", hub_routing_table);

        assert!(hub_routing_table.lock().unwrap().contains_key(cid).unwrap());

        assert!(hub_routing_table
            .lock()
            .unwrap()
            .get(cid)
            .unwrap()
            .unwrap()
            .contains_key(&PeerIdCbor::from(peer1.peer_id)));

        assert!(hub_routing_table
            .lock()
            .unwrap()
            .get(cid)
            .unwrap()
            .unwrap()
            .contains_key(&PeerIdCbor::from(peer2.peer_id)));

        assert_eq!(
            hub_routing_table
                .lock()
                .unwrap()
                .get(cid)
                .unwrap()
                .unwrap()
                .get(&PeerIdCbor::from(peer1.peer_id))
                .unwrap()
                .clone(),
            Vec::from(peer1.swarm().behaviour_mut().discovery.multiaddr.to_vec())
        );

        assert_eq!(
            hub_routing_table
                .lock()
                .unwrap()
                .get(cid)
                .unwrap()
                .unwrap()
                .get(&PeerIdCbor::from(peer2.peer_id))
                .unwrap()
                .clone(),
            Vec::from(peer2.swarm().behaviour_mut().discovery.multiaddr.to_vec())
        );

        std::thread::sleep(std::time::Duration::from_millis(1000));

        peer1.push_update(
            Vec::from(["bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq"]),
            true,
        );

        peer1.next_sync().await;

        std::thread::sleep(std::time::Duration::from_millis(1000));

        println!("hub after {:?}", hub_routing_table);

        assert!(!hub_routing_table
            .lock()
            .unwrap()
            .get(cid)
            .unwrap()
            .unwrap()
            .contains_key(&PeerIdCbor::from(peer1.peer_id)));

        assert!(hub_routing_table
            .lock()
            .unwrap()
            .get(cid)
            .unwrap()
            .unwrap()
            .contains_key(&PeerIdCbor::from(peer2.peer_id)));

        peer2.push_update(
            Vec::from(["bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq"]),
            true,
        );

        peer2.next_sync().await;

        std::thread::sleep(std::time::Duration::from_millis(1000));

        assert!(hub_routing_table.lock().unwrap().is_empty())
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
        let hub_table = hub_peer.get_routing_table();
        let hubid = hub_peer.spawn("hub");

        //  print logs for hub peer
        peer1.swarm().dial(hubid).unwrap();
        peer1.next_discovery().await;

        println!("hub  {:?}", hub_table);
        println!("peer1 before {:?}", peer1.get_routing_table());

        //  print logs for hub peer
        println!("discovery done");

        peer1.request_content(cid.unwrap());

        peer1.next_routing().await;

        let lock1 = hub_table.lock().unwrap();

        let table1 = peer1.get_routing_table();
        let lock2 = table1.lock().unwrap();

        println!("peer1 after {:?}", peer1.get_routing_table());

        assert!(lock2.contains_key(&tcid(cid.unwrap())).unwrap());
        assert_eq!(*lock1, *lock2);
    }
}
