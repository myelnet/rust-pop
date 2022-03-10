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
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::{Multiaddr, NetworkBehaviour, PeerId};
use routing::{
    PeerTable, Routing, RoutingConfig as PopConfig, RoutingEvent, RoutingTableEntry,
    SerializablePeerTable, EMPTY_QUEUE_SHRINK_THRESHOLD,
};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
pub type Index = HashMap<Cid, PeerTable>;
pub type LocalIndex = HashSet<Cid>;
pub type SerializableIndex = HashMap<Vec<u8>, SerializablePeerTable>;
pub type SerializableLocalIndex = HashSet<Vec<u8>>;
use data_transfer::{ChannelId, DataTransferBehaviour, DataTransferEvent, PullParams};
use graphsync::Graphsync;
use std::str::FromStr;

pub const ROUTING_TOPIC: &str = "myel/content-routing";
pub const SYNCING_TOPIC: &str = "myel/index-syncing";

#[derive(Debug, PartialEq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct ContentRequest {
    pub multiaddresses: Vec<Multiaddr>,
    pub root: CidCbor,
}
impl Cbor for ContentRequest {}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "RoutingEvent", poll_method = "poll", event_process = true)]
pub struct Pop<S: 'static + BlockStore>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    routing: Routing,
    data_transfer: DataTransferBehaviour<S>,

    #[behaviour(ignore)]
    pending_events: VecDeque<RoutingEvent>,
    // a map of who has the content we made requests for
    #[behaviour(ignore)]
    pending_cids: HashSet<Cid>,
    // A bidirectional map of index cids we have made data-transfer requests for.
    // This will always be empty if the node is not a hub
    #[behaviour(ignore)]
    pending_requests: BiMap<Cid, ChannelId>,
    #[behaviour(ignore)]
    store: Arc<S>,
}

impl<S: 'static + BlockStore> Pop<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(config: PopConfig, store: Arc<S>) -> Self {
        let gs = Graphsync::new(Default::default(), store.clone());
        let data_transfer = DataTransferBehaviour::new(config.peer_id, gs);

        Self {
            data_transfer,
            store,
            routing: Routing::new(config, None),
            pending_events: VecDeque::default(),
            pending_cids: HashSet::new(),
            pending_requests: BiMap::new(),
        }
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.routing.add_address(peer_id, addr.clone());
        // self.data_transfer.add_address(peer_id, addr);
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.routing.addresses_of_peer(peer_id)
    }

    fn pull_all(&mut self, peer_id: PeerId, cid: Cid) -> Result<ChannelId, String> {
        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        self.data_transfer
            .pull(peer_id, cid, selector, PullParams::default())
    }

    pub fn get(&mut self, root: Cid) -> Result<(), String> {
        //  mark that we are actively interested in content
        self.pending_cids.insert(root);
        match self.get_from_routing(root) {
            Ok(_) => Ok(()),
            Err(_) => self.routing.find_content(root),
        }
    }

    fn process_discovery_response(&mut self, peer_id: PeerId, index_root: Option<CidCbor>) {
        //  only hubs should respond
        if self.routing.config.is_hub {
            if let Some(r) = index_root {
                if let Some(cid) = r.to_cid() {
                    if self.pending_requests.get_by_left(&cid).is_none() {
                        match self.pull_all(peer_id, cid) {
                            Ok(ch) => {
                                // we make note that we've made a request for this index
                                self.pending_requests.insert(cid, ch);
                            }
                            Err(e) => println!("error in fetching index {}", e),
                        }
                    }
                }
            }
        }
    }

    fn get_from_routing(&mut self, root: Cid) -> Result<ChannelId, String> {
        // check we are still interested in a CID (i.e a transfer for it has no succesfully completed yet)
        if self.pending_cids.contains(&root) {
            //  check no transfer channel has been set up yet
            if self.pending_requests.get_by_left(&root).is_none() {
                //  if the sent Cid is valid
                let table = self.routing.routing_table.clone();
                let lock = table.read().unwrap();
                if let Some(peer_table) = lock.get(&root) {
                    let peer = peer_table.keys().next().unwrap();
                    let addresses = peer_table.get(peer).unwrap();
                    for addr in addresses {
                        self.add_address(peer, addr.clone());
                    }

                    let ch = self.pull_all(*peer, root).map_err(|e| e)?;
                    self.pending_requests.insert(root, ch.clone());
                    return Ok(ch);
                }
            }
        }
        Err("failed to fetch from routing table".to_string())
    }

    fn process_transfer_completion(
        &mut self,
        ch: ChannelId,
        res: Result<(), String>,
    ) -> Result<(), String> {
        match res {
            Ok(_) => {
                //  if we initiated the transfer
                println!("transfer succeeded");
                if ch.initiator == self.routing.config.peer_id.to_base58() {
                    //  do something when a request for a CID succeeded
                    //  if we initiated the transfer
                    println!("we initiated it {:?}", self.routing.config.peer_id);

                    if let Some(cid) = self.pending_requests.get_by_right(&ch) {
                        println!("was a pending request {:?}", cid);
                        //  we indicate that we're no longer interested in the CID so that if we get new routing responses from hubs
                        //  we simply update our routing table but don't fire off another request
                        if self.pending_cids.remove(cid) {
                            println!("was in pending cids {:?}", cid);
                            self.pending_events
                                .push_back(RoutingEvent::ContentRequestFulfilled(cid.to_string()));
                        } else {
                            //  do something when a request for an index CID succeeded
                            let raw_bytes =
                                dag_service::cat(self.store.clone(), *cid).map_err(|e| e)?;
                            let entry = RoutingTableEntry::unmarshal_cbor(&raw_bytes)
                                .map_err(|e| e.to_string())?;
                            //  override just in case
                            if let Ok(p) = PeerId::from_str(&ch.responder) {
                                self.routing.insert_routing_entry(p, entry).map_err(|e| e)?;
                                self.pending_events.push_back(RoutingEvent::HubIndexUpdated);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                println!("transfer error: {:?}", e);
                //  do something when a request for a CID failed
            }
        }

        self.pending_requests.remove_by_right(&ch);

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

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<DataTransferEvent> for Pop<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: DataTransferEvent) {
        println!("data transfer event {:?}", event);
        match event {
            DataTransferEvent::Completed(ch, res) => {
                self.process_transfer_completion(ch, res).unwrap();
            }
            _ => {}
        }
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<RoutingEvent> for Pop<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: RoutingEvent) {
        match event {
            RoutingEvent::HubTableUpdated(peer, root) => {
                self.process_discovery_response(peer, root.clone());
                self.pending_events
                    .push_back(RoutingEvent::HubTableUpdated(peer, root));
            }
            RoutingEvent::RoutingTableUpdated(root) => {
                println!("Routing Table Updated {:?}", root);
                self.get_from_routing(root);
            }
            e => self.pending_events.push_back(e),
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
        swarm: Swarm<Pop<MemoryBlockStore>>,
    }

    impl Peer {
        fn new(is_hub: bool) -> Self {
            let (peer_id, trans) = mk_transport();
            let bs = Arc::new(MemoryBlockStore::default());
            let config = PopConfig { peer_id, is_hub };
            let rt = Pop::new(config, bs);
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
            return self.swarm.behaviour_mut().routing.routing_table.clone();
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn get(&mut self, content: Cid) {
            self.swarm().behaviour_mut().get(content).unwrap();
        }

        fn make_index(&mut self) -> RoutingTableEntry {
            // generate 1 random byte
            const FILE_SIZE: usize = 1;
            let mut data = vec![0u8; FILE_SIZE];
            rand::thread_rng().fill_bytes(&mut data);

            let root = dag_service::add(self.swarm().behaviour_mut().store.clone(), &data)
                .unwrap()
                .unwrap();

            let entry = RoutingTableEntry::Insertion {
                multiaddresses: Vec::from([self.addr.clone()]),
                cids: Vec::from([CidCbor::from(root)]),
            };

            let data = entry.marshal_cbor().unwrap();

            if let Some(cid) =
                dag_service::add(self.swarm().behaviour_mut().store.clone(), &data).unwrap()
            {
                self.swarm()
                    .behaviour_mut()
                    .routing
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
                        RoutingEvent::RoutingTableUpdated(_) => return Some(event),
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
                        _ => println!("{:?}", event),
                    }
                }
            }
        }
    }

    #[async_std::test]
    async fn test_can_make_request() {
        //  will have empty hub table as not a hub
        let mut peer1 = Peer::new(false);

        // will have themselves in hub table
        let mut peer2 = Peer::new(false);
        // will have themselves in hub table
        let peer3 = Peer::new(false);
        let entry = peer2.make_index();

        peer1.add_address(&peer2);

        let peer2id = peer2.spawn("peer2");

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let cid = match entry {
            RoutingTableEntry::Insertion {
                multiaddresses: _,
                cids,
            } => Some(cids.first().unwrap().to_cid().unwrap()),
            _ => None,
        };

        peer1
            .swarm()
            .behaviour_mut()
            .data_transfer
            .pull(peer2id, cid.unwrap(), selector, PullParams::default())
            .unwrap();

        peer1.next_indexing().await;
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

        let res = match entry {
            RoutingTableEntry::Insertion {
                multiaddresses: ma,
                cids,
            } => Some((cids.first().unwrap().to_cid().unwrap(), ma)),
            _ => None,
        };
        let (cid, ma) = res.unwrap();

        assert!(hub_routing_table.read().unwrap().contains_key(&cid));

        assert!(hub_routing_table
            .read()
            .unwrap()
            .get(&cid)
            .unwrap()
            .contains_key(&peer2id));

        assert_eq!(
            hub_routing_table
                .read()
                .unwrap()
                .get(&cid)
                .unwrap()
                .get(&peer2id)
                .unwrap()
                .clone(),
            SmallVec::<[Multiaddr; 4]>::from(ma)
        );
    }

    #[async_std::test]
    async fn test_content_request() {
        let mut hub_peer = Peer::new(true);
        // Peer 1 has a content table that its going to push to hub peer
        let mut peer1 = Peer::new(false);
        let entry = peer1.make_index();
        let cid = match entry {
            RoutingTableEntry::Insertion {
                multiaddresses: _,
                cids,
            } => Some(cids.first().unwrap().to_cid().unwrap()),
            _ => None,
        }
        .unwrap();

        hub_peer.add_address(&peer1);
        //  print logs for hub peer
        let peer1id = peer1.spawn("peer1");

        //  print logs for hub peer
        hub_peer.swarm().dial(peer1id).unwrap();
        hub_peer.next_indexing().await;

        hub_peer.get(cid);

        let cid_resp = hub_peer.next_content_fulfilled().await;

        // assert the content table updated correctly
        assert_eq!(cid.to_string(), cid_resp.unwrap());
    }

    #[async_std::test]
    async fn test_full_loop() {
        let mut hub_peer = Peer::new(true);
        // Peer 1 has a content table that its going to push to hub peer
        let mut peer1 = Peer::new(false);
        let entry = peer1.make_index();
        let cid = match entry {
            RoutingTableEntry::Insertion {
                multiaddresses: _,
                cids,
            } => Some(cids.first().unwrap().to_cid().unwrap()),
            _ => None,
        }
        .unwrap();

        let mut peer2 = Peer::new(false);

        hub_peer.add_address(&peer1);
        peer2.add_address(&hub_peer);
        peer2.add_address(&peer1);

        //  print logs for hub peer
        let peer1id = peer1.spawn("peer1");

        //  print logs for hub peer
        hub_peer.swarm().dial(peer1id).unwrap();
        hub_peer.next_indexing().await;

        let hubid = hub_peer.spawn("hub");

        //  print logs for hub peer
        peer2.swarm().dial(hubid).unwrap();
        //  print logs for hub peer
        // peer2.swarm().dial(peer1id).unwrap();
        peer2.next_discovery().await;
        // peer2.next_discovery().await;
        //  print logs for hub peer
        // peer2.swarm().dial(peer1id).unwrap();
        peer2.get(cid);

        // //  print logs for hub peer
        // peer2.next_routing().await;

        let cid_resp = peer2.next_content_fulfilled().await;

        // assert the content table updated correctly
        assert_eq!(cid.to_string(), cid_resp.unwrap());
    }
}
