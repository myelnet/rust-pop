#[cfg(feature = "browser")]
mod browser;
#[cfg(feature = "browser")]
pub use self::browser::*;
#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
pub use self::native::node::*;

use bimap::BiMap;
use blockstore::types::BlockStore;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use graphsync::traversal::{RecursionLimit, Selector};
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use libp2p::gossipsub::{Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::{Multiaddr, NetworkBehaviour, PeerId};
use routing::gossip_init;
use routing::{DiscoveryConfig, DiscoveryEvent, HubDiscovery, PeerTable, SerializablePeerTable};
use routing::{RoutingNetEvent, RoutingNetwork, RoutingRecord, EMPTY_QUEUE_SHRINK_THRESHOLD};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

pub type Index = HashMap<Cid, PeerTable>;
pub type LocalIndex = HashSet<Cid>;
pub type SerializableIndex = HashMap<Vec<u8>, SerializablePeerTable>;
pub type SerializableLocalIndex = HashSet<Vec<u8>>;

pub const ROUTING_TOPIC: &str = "myel/content-routing";
pub const SYNCING_TOPIC: &str = "myel/index-syncing";

#[derive(Debug, PartialEq, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u64)]
pub enum MessageType {
    Insertion = 0,
    Deletion = 1,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RoutingTableEntry {
    pub multiaddresses: Vec<Multiaddr>,
    pub cids: Vec<CidCbor>,
    pub update: MessageType,
}
impl Cbor for RoutingTableEntry {}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ContentRequest {
    pub multiaddresses: Vec<Multiaddr>,
    pub root: CidCbor,
}
impl Cbor for ContentRequest {}

#[derive(Debug)]
pub enum RoutingEvent {
    ContentRequestBroadcast(String),
    FoundContent(String),
    ContentRequestFulfilled(String),
    SyncRequestBroadcast,
    HubTableUpdated,
    HubIndexUpdated,
    RoutingTableUpdated,
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "RoutingEvent", poll_method = "poll", event_process = true)]
pub struct Pop<S: 'static + BlockStore>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    discovery: HubDiscovery,
    broadcaster: Gossipsub,
    routing_responder: RoutingNetwork,

    #[behaviour(ignore)]
    peer_id: PeerId,
    #[behaviour(ignore)]
    is_hub: bool,
    #[behaviour(ignore)]
    pending_events: VecDeque<RoutingEvent>,
    // a map of who has the content we made requests for
    #[behaviour(ignore)]
    routing_table: Arc<RwLock<Index>>,
    // A map of cids we want to route to.
    #[behaviour(ignore)]
    pending_cids: HashSet<Cid>,
    // A bidirectional map of cids we have made data-transfer requests for.
    #[behaviour(ignore)]
    store: Arc<S>,
}

impl<S: 'static + BlockStore> Pop<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(peer_id: PeerId, is_hub: bool, store: Arc<S>, index_root: Option<CidCbor>) -> Self {
        let discovery = HubDiscovery::new(DiscoveryConfig::default(), peer_id, is_hub, index_root);
        //  topic with identity hash
        let broadcaster = gossip_init(
            peer_id,
            Vec::from([
                IdentTopic::new(ROUTING_TOPIC),
                IdentTopic::new(SYNCING_TOPIC),
            ]),
        );

        Self {
            peer_id,
            is_hub,
            discovery,
            broadcaster,
            store,
            routing_responder: RoutingNetwork::new(),
            pending_events: VecDeque::default(),
            // can swap this for something on disk, this is a thread safe hashmap
            routing_table: Arc::new(RwLock::new(HashMap::new())),
            pending_cids: HashSet::new(),
        }
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.discovery.add_address(peer_id, addr);
        // self.broadcaster.add_explicit_peer(peer_id);
    }

    pub fn broadcast_update(
        &mut self,
        cids: Vec<CidCbor>,
        update: MessageType,
    ) -> Result<(), String> {
        let msg = RoutingTableEntry {
            multiaddresses: self.discovery.multiaddr.to_vec(),
            cids,
            update,
        }
        .marshal_cbor()
        .map_err(|e| e.to_string())?;
        // Because Topic is not thread safe (the hasher it uses can't be safely shared across threads)
        // we create a new instantiation of the Topic for each publication, in most examples Topic is
        // cloned anyway
        self.broadcaster
            .publish(IdentTopic::new(SYNCING_TOPIC), msg)
            .map_err(|e| e.to_string())?;
        self.pending_events
            .push_back(RoutingEvent::SyncRequestBroadcast);

        Ok(())
    }

    pub fn find_content(&mut self, root: Cid) -> Result<(), String> {
        let msg = ContentRequest {
            multiaddresses: self.discovery.multiaddr.to_vec(),
            root: CidCbor::from(root),
        }
        .marshal_cbor()
        .map_err(|e| e.to_string())?;

        //  mark that we are actively interested in content
        self.pending_cids.insert(root);
        // false flags that we haven't made a request for the content yet
        let mut sent_request = false;
        //  there's no pending request for the content
        //  we have an entry in our routing table
        if self.routing_table.read().unwrap().contains_key(&root) {
            ()
        }

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

    fn insert_routing_entry(
        &mut self,
        peer_id: PeerId,
        entry: RoutingTableEntry,
    ) -> Result<(), String> {
        if entry.update == MessageType::Insertion {
            let mut addr_vec = SmallVec::<[Multiaddr; 6]>::new();
            // check associated multiaddresses are valid
            for addr in entry.multiaddresses {
                addr_vec.push(addr)
            }
            let peer_table: PeerTable = PeerTable {
                peers: HashMap::from([(peer_id, addr_vec)]),
            };
            let mut lock = self.routing_table.write().unwrap();
            for cid in entry.cids {
                // check sent cids iare valid
                if let Some(c) = cid.to_cid() {
                    lock.insert(c, peer_table.clone());
                }
                // quit if a single CID is invalid, this can be relaxed
                else {
                    return Err("contained an invalid cid".to_string());
                }
            }
        } else if entry.update == MessageType::Deletion {
            for cid in entry.cids {
                // check sent cids are valid
                let mut lock = self.routing_table.write().unwrap();
                if let Some(c) = cid.to_cid() {
                    if let Some(peer_table) = lock.get_mut(&c) {
                        peer_table.peers.remove(&peer_id);
                        // if empty clear the entry for the CID
                        if peer_table.peers.is_empty() {
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
        Ok(())
    }

    fn process_discovery_response(&mut self, peer_id: PeerId, index_root: Option<CidCbor>) {
        self.pending_events.push_back(RoutingEvent::HubTableUpdated);
    }

    fn process_routing_response(&mut self, peer_table: PeerTable, root: Cid) {
        // expand table as new entries come in
        let mut new_entry = HashMap::new();
        new_entry.insert(root, peer_table);
        self.routing_table.write().unwrap().extend(new_entry);
        self.pending_events
            .push_back(RoutingEvent::RoutingTableUpdated);
    }

    fn process_routing_request(
        &mut self,
        peer_id: PeerId,
        message: GossipsubMessage,
    ) -> Result<(), String> {
        // only hubs should respond
        if self.is_hub {
            let req = ContentRequest::unmarshal_cbor(&message.data).map_err(|e| e.to_string())?;
            //  if the sent Cid is valid
            if let Some(r) = req.root.to_cid() {
                if !self.store.contains(&r).map_err(|e| e.to_string())? {
                    //  if routing table contains the root cid, if not then do nothing
                    if let Some(peer_table) = self.routing_table.read().unwrap().get(&r) {
                        let message = RoutingRecord {
                            root: req.root,
                            peers: Some(SerializablePeerTable::from(peer_table.clone())),
                        };
                        self.routing_responder.send_message(&peer_id, message);
                        self.pending_events
                            .push_back(RoutingEvent::FoundContent(r.to_string()));
                    }
                } else {
                    return Err("invalid cid".to_string());
                }
            }
        }

        Ok(())
    }

    fn process_sync_request(
        &mut self,
        peer_id: PeerId,
        message: GossipsubMessage,
    ) -> Result<(), String> {
        // only hubs should respond
        if self.is_hub {
            let entry =
                RoutingTableEntry::unmarshal_cbor(&message.data).map_err(|e| e.to_string())?;
            self.insert_routing_entry(peer_id, entry).map_err(|e| e)?;
            self.pending_events.push_back(RoutingEvent::HubIndexUpdated);
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

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<DiscoveryEvent> for Pop<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::ResponseReceived(_, peer, root) => {
                self.process_discovery_response(peer, root)
            }
        }
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<RoutingNetEvent> for Pop<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: RoutingNetEvent) {
        match event {
            RoutingNetEvent::Response(_, resp) => {
                if let Some(peers) = resp.peers {
                    if let Some(r) = resp.root.to_cid() {
                        self.process_routing_response(PeerTable::from(peers), r)
                    }
                }
            }
        }
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<GossipsubEvent> for Pop<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
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
            GossipsubEvent::Subscribed { .. } => {}
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
    use graphsync::Graphsync;
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
        swarm: Swarm<Pop<MemoryBlockStore>>,
    }

    impl Peer {
        fn new(is_hub: bool) -> Self {
            let (peer_id, trans) = mk_transport();
            let bs = Arc::new(MemoryBlockStore::default());
            let gs = Graphsync::new(Default::default(), bs.clone());
            let rt = Pop::new(peer_id, is_hub, bs.clone(), None);
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

        fn push_update(&mut self, content: Vec<&str>, msg: MessageType) -> Vec<CidCbor> {
            let mut cids = Vec::new();
            for cid in content {
                cids.push(CidCbor::from(Cid::try_from(cid).unwrap()));
            }
            self.swarm()
                .behaviour_mut()
                .broadcast_update(cids.clone(), msg)
                .unwrap();
            cids
        }

        fn make_index(&mut self) -> RoutingTableEntry {
            // generate 1 random byte
            const FILE_SIZE: usize = 1;
            let mut data = vec![0u8; FILE_SIZE];
            rand::thread_rng().fill_bytes(&mut data);

            let root = dag_service::add(self.swarm().behaviour_mut().store.clone(), &data)
                .unwrap()
                .unwrap();

            let entry = RoutingTableEntry {
                multiaddresses: Vec::from([self.addr.clone()]),
                cids: Vec::from([CidCbor::from(root)]),
                update: MessageType::Insertion,
            };

            let data = entry.marshal_cbor().unwrap();

            if let Some(cid) =
                dag_service::add(self.swarm().behaviour_mut().store.clone(), &data).unwrap()
            {
                self.swarm()
                    .behaviour_mut()
                    .discovery
                    .update_index_root(CidCbor::from(cid));
            }

            entry
        }

        fn swarm(&mut self) -> &mut Swarm<Pop<MemoryBlockStore>> {
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

        async fn next_discovery(&mut self) -> Option<RoutingEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        RoutingEvent::HubTableUpdated => return Some(event),
                        _ => println!("{:?}", event),
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
                        RoutingEvent::RoutingTableUpdated => return Some(event),
                        _ => {}
                    }
                }
            }
        }

        async fn next_content_fulfilled(&mut self) -> Option<String> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        RoutingEvent::ContentRequestFulfilled(cid) => return Some(cid),
                        _ => {}
                    }
                }
            }
        }
    }

    #[async_std::test]
    async fn test_can_sync_index_on_hanshake() {
        //  will have empty hub table as not a hub
        let mut peer1 = Peer::new(true);

        // will have themselves in hub table
        let mut peer2 = Peer::new(false);
        let entry = peer2.make_index();

        peer1.add_address(&peer2);

        let hub_routing_table = peer1.get_routing_table().clone();

        let peer2id = peer2.spawn("peer2");

        let dial = peer1.swarm().dial(peer2id);

        println!("dial {:?}", dial);

        assert!(dial.is_ok());

        peer1.next_indexing().await;

        let cid = &entry.cids.first().unwrap().to_cid().unwrap();

        assert!(hub_routing_table.read().unwrap().contains_key(cid));

        assert!(hub_routing_table
            .read()
            .unwrap()
            .get(cid)
            .unwrap()
            .peers
            .contains_key(&peer2id));

        assert_eq!(
            hub_routing_table
                .read()
                .unwrap()
                .get(cid)
                .unwrap()
                .peers
                .get(&peer2id)
                .unwrap()
                .clone(),
            SmallVec::<[Multiaddr; 6]>::from(entry.multiaddresses)
        );
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
            MessageType::Insertion,
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
            .peers
            .contains_key(&peer1.peer_id));

        assert_eq!(
            hub_routing_table
                .read()
                .unwrap()
                .get(cid)
                .unwrap()
                .peers
                .get(&peer1.peer_id)
                .unwrap()
                .clone(),
            SmallVec::<[Multiaddr; 6]>::from(
                peer1.swarm().behaviour_mut().discovery.multiaddr.to_vec()
            )
        );

        peer1.push_update(
            Vec::from(["bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq"]),
            MessageType::Deletion,
        );

        peer1.next_sync().await;

        std::thread::sleep(std::time::Duration::from_millis(1000));

        assert!(hub_routing_table.read().unwrap().is_empty())
    }

    #[async_std::test]
    async fn test_content_request() {
        let mut hub_peer = Peer::new(true);
        // Peer 1 has a content table that its going to push to hub peer
        let mut peer1 = Peer::new(false);
        let entry = peer1.make_index();
        let cid = &entry.cids.first().unwrap().to_cid().unwrap();
        let mut peer2 = Peer::new(false);

        hub_peer.add_address(&peer1);
        peer2.add_address(&hub_peer);

        //  print logs for hub peer
        let hub_table = hub_peer.get_routing_table().clone();
        let peer1id = peer1.spawn("peer1");

        //  print logs for hub peer
        hub_peer.swarm().dial(peer1id).unwrap();
        hub_peer.next_indexing().await;

        let hubid = hub_peer.spawn("hub");

        //  print logs for hub peer
        peer2.swarm().dial(hubid).unwrap();
        peer2.next_discovery().await;
        println!("discovery done");

        peer2.request_content(*cid);

        peer2.next_routing().await;

        let lock1 = hub_table.read().unwrap();

        let table2 = peer2.get_routing_table();
        let lock2 = table2.read().unwrap();

        let mut k1: Vec<&Cid> = lock1.keys().collect();
        let mut k2: Vec<&Cid> = lock2.keys().collect();

        assert_eq!(k1.sort(), k2.sort());

        let v1: Vec<&HashMap<PeerId, SmallVec<[Multiaddr; 6]>>> =
            lock1.values().map(|x| &x.peers).collect();
        let v3: Vec<&HashMap<PeerId, SmallVec<[Multiaddr; 6]>>> =
            lock2.values().map(|x| &x.peers).collect();

        assert_eq!(v1, v3);

        let cid_resp = peer2.next_content_fulfilled().await;

        // assert the content table updated correctly
        assert_eq!(cid.to_string(), cid_resp.unwrap());
    }
}
