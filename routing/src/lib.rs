mod hub_discovery;
mod hub_indexing;
mod network;
mod utils;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use hub_discovery::Config as DiscoveryConfig;
use hub_discovery::{DiscoveryEvent, HubDiscovery, PeerTable};
use hub_indexing::Config as IndexingConfig;
use hub_indexing::{ContentTable, HubIndexing, IndexingEvent};
use libipld::Cid;
use libp2p::gossipsub::{Gossipsub, GossipsubEvent, GossipsubMessage, Hasher, IdentTopic};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::{Multiaddr, NetworkBehaviour, PeerId};
use network::{PeersforContent, RoutingNetEvent, RoutingNetwork, EMPTY_QUEUE_SHRINK_THRESHOLD};
use serde::{Deserialize, Serialize};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::collections::hash_map::DefaultHasher;
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
    pub fn new(peer_id: PeerId, is_hub: bool) -> Self {
        let hub_table = Arc::new(RwLock::new(HashMap::new()));
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
