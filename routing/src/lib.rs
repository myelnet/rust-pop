mod discovery;
mod routing;
mod utils;
use bimap::BiMap;
use blockstore::types::BlockStore;
use data_transfer::{ChannelId, DataTransfer, DataTransferEvent, DealParams};
use discovery::Config as DiscoveryConfig;
use discovery::{DiscoveryEvent, HubDiscovery, PeerTable, SerializablePeerTable};
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
use routing::{RoutingNetEvent, RoutingNetwork, RoutingProposal, EMPTY_QUEUE_SHRINK_THRESHOLD};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use utils::gossip_init;

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
    pub peer: Vec<u8>,
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
    SyncRequestBroadcast,
    HubTableUpdated,
    HubIndexUpdated,
    RoutingTableUpdated,
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "RoutingEvent", poll_method = "poll", event_process = true)]
pub struct Routing<S: 'static + BlockStore>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    discovery: HubDiscovery,
    syncing_broadcaster: Gossipsub,
    data_transfer: DataTransfer<S>,
    routing_broadcaster: Gossipsub,
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
    #[behaviour(ignore)]
    pending_cid_requests: BiMap<Cid, ChannelId>,
    // A bidirectional map of index cids we have made data-transfer requests for.
    #[behaviour(ignore)]
    pending_index_requests: BiMap<Cid, ChannelId>,
    #[behaviour(ignore)]
    store: Arc<S>,
}

impl<S: 'static + BlockStore> Routing<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(
        peer_id: PeerId,
        is_hub: bool,
        data_transfer: DataTransfer<S>,
        store: Arc<S>,
    ) -> Self {
        let discovery = HubDiscovery::new(DiscoveryConfig::default(), peer_id, is_hub, None);
        //  topic with identity hash
        let routing_broadcaster = gossip_init(peer_id, IdentTopic::new(ROUTING_TOPIC));
        let syncing_broadcaster = gossip_init(peer_id, IdentTopic::new(SYNCING_TOPIC));

        Self {
            peer_id,
            is_hub,
            discovery,
            syncing_broadcaster,
            data_transfer,
            routing_broadcaster,
            store,
            routing_responder: RoutingNetwork::new(),
            pending_events: VecDeque::default(),
            // can swap this for something on disk, this is a thread safe hashmap
            routing_table: Arc::new(RwLock::new(HashMap::new())),
            pending_cids: HashSet::new(),
            pending_cid_requests: BiMap::new(),
            pending_index_requests: BiMap::new(),
        }
    }

    pub fn broadcast_update(
        &mut self,
        cids: Vec<CidCbor>,
        update: MessageType,
    ) -> Result<(), String> {
        let msg = RoutingTableEntry {
            multiaddresses: self.discovery.multiaddr.to_vec(),
            peer: self.peer_id.to_bytes(),
            cids: cids.clone(),
            update,
        }
        .marshal_cbor()
        .map_err(|e| e.to_string())?;
        // Because Topic is not thread safe (the hasher it uses can't be safely shared across threads)
        // we create a new instantiation of the Topic for each publication, in most examples Topic is
        // cloned anyway
        self.routing_broadcaster
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
        if self.pending_cid_requests.get_by_left(&root).is_none() {
            //  we have an entry in our routing table
            if self.routing_table.read().unwrap().contains_key(&root) {
                match self.fetch_from_random_peer(root) {
                    Ok(ch) => {
                        sent_request = true;
                        self.pending_cid_requests.insert(root, ch);
                    }
                    Err(e) => println!("{}", e),
                }
            }
        } else {
            // we've already made a data-transfer request for the content
            sent_request = true
        }
        if !sent_request {
            // Because Topic is not thread safe (the hasher it uses can't be safely shared across threads)
            // we create a new instantiation of the Topic for each publication, in most examples Topic is
            // cloned anyway
            self.routing_broadcaster
                .publish(IdentTopic::new(ROUTING_TOPIC), msg)
                .map_err(|e| e.to_string())?;
            self.pending_events
                .push_back(RoutingEvent::ContentRequestBroadcast(root.to_string()));
        }

        Ok(())
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.discovery.add_address(peer_id, addr);
        self.routing_broadcaster.add_explicit_peer(peer_id);
        self.syncing_broadcaster.add_explicit_peer(peer_id);
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.discovery.addresses_of_peer(peer_id)
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
            let peer_table: PeerTable = HashMap::from([(peer_id, addr_vec)]);
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
                // check sent cids iare valid
                let mut lock = self.routing_table.write().unwrap();
                if let Some(c) = cid.to_cid() {
                    if let Some(peer_table) = lock.get_mut(&c) {
                        peer_table.remove(&peer_id);
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

    fn delete_routing_entry(&mut self, root: &Cid) {
        self.routing_table.write().unwrap().remove(root);
    }

    fn reset_routing_table(&mut self) {
        self.routing_table = Arc::new(RwLock::new(HashMap::new()));
    }

    fn process_transfer_completion(
        &mut self,
        ch: ChannelId,
        res: Result<(), String>,
    ) -> Result<(), String> {
        match res {
            Ok(_) => {
                //  do something when a request for a CID succeeded
                if let Some(cid) = self.pending_cid_requests.get_by_right(&ch) {
                    //  we indicate that we're no longer interested in the CID so that if we get new routing responses from hubs
                    //  we simply update our routing table but don't fire off another request
                    self.pending_cids.remove(cid);
                }
                //  do something when a request for an index CID succeeded
                else if let Some(cid) = self.pending_index_requests.get_by_right(&ch) {
                    let raw_bytes = dag_service::cat(self.store.clone(), *cid).map_err(|e| e)?;
                    let mut entry =
                        RoutingTableEntry::unmarshal_cbor(&raw_bytes).map_err(|e| e.to_string())?;
                    //  override just in case
                    entry.update = MessageType::Insertion;
                    if let Ok(p) = PeerId::from_str(&ch.responder) {
                        self.insert_routing_entry(p, entry).map_err(|e| e)?;
                    }
                }

                //  can safely remove from both as no error is thrown if the element is not found
                self.pending_cid_requests.remove_by_right(&ch);
                self.pending_index_requests.remove_by_right(&ch);
                Ok(())
            }
            Err(e) => {
                //  do something when a request for a CID failed e
                if self.pending_cid_requests.contains_right(&ch) {
                    self.pending_cid_requests.remove_by_right(&ch);
                    // eg. may want to restart transfer with a new peer
                }
                //  do something when a request for an index CID failed
                else if self.pending_index_requests.contains_right(&ch) {
                    self.pending_index_requests.remove_by_right(&ch);
                }
                Ok(())
            }
        }
    }

    fn process_discovery_response(&mut self, peer_bytes: Vec<u8>, index_root: Option<CidCbor>) {
        self.pending_events.push_back(RoutingEvent::HubTableUpdated);
        // only hubs should respond
        if self.is_hub {
            if let Ok(peer_id) = PeerId::try_from(peer_bytes) {
                if let Some(r) = index_root {
                    if let Some(cid) = r.to_cid() {
                        if self.pending_index_requests.get_by_left(&cid).is_none() {
                            match self.fetch_content(peer_id, cid) {
                                Ok(ch) => {
                                    // we make note that we've made a request for this index
                                    self.pending_index_requests.insert(cid, ch);
                                }
                                Err(e) => println!("{}", e),
                            }
                        }
                    }
                }
            }
        }
    }

    fn fetch_content(&mut self, peer_id: PeerId, cid: Cid) -> Result<ChannelId, String> {
        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        self.data_transfer
            .pull(peer_id, cid, selector, DealParams::default())
    }

    fn fetch_from_random_peer(&mut self, cid: Cid) -> Result<ChannelId, String> {
        let table = self.routing_table.clone();
        let lock = table.read().unwrap();
        if let Some(peer_table) = lock.get(&cid) {
            for peer in peer_table.keys() {
                for addr in peer_table.get(peer).unwrap() {
                    self.add_address(peer, addr.clone());
                }
                match self.fetch_content(*peer, cid) {
                    Ok(ch) => return Ok(ch),
                    Err(e) => println!("Failed to fetch {:?} from {:?}", cid, peer),
                }
            }
        }
        Err("failed to fetch from all peers".to_string())
    }

    fn process_routing_response(&mut self, peer_table: PeerTable, root: Cid) {
        // expand table as new entries come in
        let mut new_entry = HashMap::new();
        new_entry.insert(root, peer_table);
        self.routing_table.write().unwrap().extend(new_entry);
        self.pending_events
            .push_back(RoutingEvent::RoutingTableUpdated);
        // check we are still interested in a CID (i.e a transfer for it has no succesfully completed yet)
        if self.pending_cids.contains(&root) {
            //  check no transfer channel has been set up yet
            if self.pending_cid_requests.get_by_left(&root).is_none() {
                //  if the sent Cid is valid
                match self.fetch_from_random_peer(root) {
                    Ok(ch) => {
                        // flag that a request for the cid has now been made
                        self.pending_cid_requests.insert(root, ch);
                    }
                    Err(e) => println!("{}", e),
                }
            }
        }
    }

    fn process_routing_request(
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
                if !self
                    .store
                    .contains(&r)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                {
                    //  if routing table contains the root cid, if not then do nothing
                    if let Some(peer_table) = self.routing_table.read().unwrap().get(&r) {
                        let message = RoutingProposal {
                            root: req.root,
                            peers: Some(utils::peer_table_to_bytes(peer_table)),
                        };
                        self.routing_responder.send_message(&peer_id, message);
                        self.pending_events
                            .push_back(RoutingEvent::FoundContent(r.to_string()));
                    }
                } else {
                    return Err(io::Error::new(io::ErrorKind::Other, "invalid cid"));
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

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<DiscoveryEvent> for Routing<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::ResponseReceived(resp) => {
                self.process_discovery_response(resp.source, resp.index_root)
            }
        }
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<DataTransferEvent> for Routing<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: DataTransferEvent) {
        match event {
            DataTransferEvent::Completed(ch, res) => {
                self.process_transfer_completion(ch, res).unwrap();
            }
            _ => {}
        }
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<RoutingNetEvent> for Routing<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: RoutingNetEvent) {
        match event {
            RoutingNetEvent::Response(_, resp) => {
                if let Some(peers) = resp.peers {
                    if let Some(r) = resp.root.to_cid() {
                        self.process_routing_response(utils::peer_table_from_bytes(&peers), r)
                    }
                }
            }
        }
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<GossipsubEvent> for Routing<S>
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
        swarm: Swarm<Routing<MemoryBlockStore>>,
        // store: Arc<MemoryBlockStore>,
    }

    impl Peer {
        fn new(is_hub: bool) -> Self {
            let (peer_id, trans) = mk_transport();
            let bs = Arc::new(MemoryBlockStore::default());
            let gs = Graphsync::new(Default::default(), bs.clone());
            let dt = DataTransfer::new(peer_id, gs);
            let rt = Routing::new(peer_id, is_hub, dt, bs);
            let mut swarm = Swarm::new(trans, rt, peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
                // store: bs,
            }
        }

        fn get_hub_table(&mut self) -> Arc<RwLock<PeerTable>> {
            return self.swarm.behaviour_mut().discovery.hub_table.clone();
        }

        fn get_index(&mut self) -> Arc<RwLock<Index>> {
            return self.swarm.behaviour_mut().routing_table.clone();
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn swarm(&mut self) -> &mut Swarm<Routing<MemoryBlockStore>> {
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
    }

    #[async_std::test]
    async fn test_peer_discovery() {
        //  will have empty hub table as not a hub
        let mut peer1 = Peer::new(false);
        // will have themselves in hub table
        let peer2 = Peer::new(true);
        let mut peer3 = Peer::new(true);

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

    // #[async_std::test]
    // async fn test_index_update() {
    //     let cid = Cid::try_from("bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq")
    //         .unwrap();
    //     let content = CidCbor::from(cid);
    //     let host_peer = Peer::new(true);
    //     let multiaddr = SmallVec::<[Multiaddr; 6]>::from(Vec::from([host_peer.addr]));
    //     let peer_id = host_peer.peer_id;
    //     let peer_table: PeerTable = HashMap::from([(peer_id, multiaddr)]);
    //
    //     let mut hub_peer = Peer::new(true);
    //     let mut peer1 = Peer::new(false);
    //
    //     // peer1
    //     //     .swarm()
    //     //     .behaviour_mut()
    //     //     .insert_routing_entry(content, peer_table);
    //
    //     let mut peer2 = Peer::new(false);
    //
    //     peer1.add_address(&hub_peer);
    //     peer1.add_address(&peer2);
    //
    //     //  print logs for hub peer
    //     let hub_table = hub_peer.get_index().clone();
    //     let hubid = hub_peer.spawn("hub");
    //
    //     peer1.swarm().dial(hubid).unwrap();
    //     peer1.next_discovery().await;
    //
    //     // update remote hubs
    //     // peer1.swarm().behaviour_mut().broadcast_update();
    //     peer1.next_indexing().await;
    //
    //     // assert peer 2 table did not update
    //     assert!(peer2.get_index().is_empty());
    //
    //     assert_eq!(hub_table, peer1.get_index());
    // }
    // #[async_std::test]
    // async fn test_routing_broadcaster() {
    //     let cid = Cid::try_from("bafy2bzaceafciokjlt5v5l53pftj6zcmulc2huy3fduwyqsm3zo5bzkau7muq")
    //         .unwrap();
    //     // let content = CidCbor::from(cid);
    //     let host_peer = Peer::new(true);
    //     let multiaddr = SmallVec::<[Multiaddr; 6]>::from(Vec::from([host_peer.addr]));
    //     let peer_id = host_peer.peer_id;
    //     let peer_table: PeerTable = HashMap::from([(peer_id, multiaddr)]);
    //
    //     let mut hub_peer = Peer::new(true);
    //     // Peer 1 has a content table that its going to push to hub peer
    //     let mut peer1 = Peer::new(false);
    //     // peer1
    //     //     .swarm()
    //     //     .behaviour_mut()
    //     //     .insert_routing_entry(content.clone(), peer_table);
    //
    //     //  peers 2 and 3 are unaware of the hub, but peer 1 and 4 are aware of it
    //     let peer2 = Peer::new(false);
    //     let peer3 = Peer::new(false);
    //     let mut peer4 = Peer::new(false);
    //
    //     peer1.add_address(&hub_peer);
    //     peer1.add_address(&peer2);
    //     peer1.add_address(&peer3);
    //     peer1.add_address(&peer4);
    //
    //     peer4.add_address(&hub_peer);
    //
    //     //  print logs for hub peer
    //     let hub_table = hub_peer.get_index().clone();
    //     let hubid = hub_peer.spawn("hub");
    //
    //     //  print logs for hub peer
    //     peer1.swarm().dial(hubid).unwrap();
    //     peer1.next_discovery().await;
    //
    //     // update remote hubs
    //     // peer1.swarm().behaviour_mut().broadcast_update();
    //     peer1.next_indexing().await;
    //
    //     assert_eq!(hub_table, peer1.get_index());
    //
    //     //  print logs for hub peer
    //     peer4.swarm().dial(hubid).unwrap();
    //     peer4.next_discovery().await;
    //     peer4.swarm().behaviour_mut().find_content(cid).unwrap();
    //     peer4.next_routing().await;
    //
    //     // assert the content table updated correctly
    //     assert_eq!(hub_table, peer4.get_index());
    // }
}
