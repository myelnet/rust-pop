use blockstore::types::BlockStore;
use dag_service;
use data_transfer::{DataTransfer, DataTransferEvent, DealParams};
use graphsync::traversal::{RecursionLimit, Selector};
use graphsync::{Config as GraphsyncConfig, Graphsync};
use instant::Instant;
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::Cid;
use libipld::Ipld;
use libp2p::futures::StreamExt;
use libp2p::swarm::SwarmBuilder;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{self, identity, Multiaddr, PeerId};
use rand::prelude::*;
use routing::{Config as PeerDiscoveryConfig, PeerDiscovery};
use std::sync::Arc;
use wasm_bindgen_futures;
use crate::browser::build_transport;


const DATA_SIZE: usize = 104857600;
pub struct Node<B: 'static + BlockStore>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    pub swarm: Swarm<DataTransfer<B>>,
    pub store: Arc<B>,
}

#[derive(Debug)]
pub struct NodeConfig<B: 'static + BlockStore> {
    pub listening_multiaddr: Option<Multiaddr>,
    pub blockstore: B,
}

impl<B: BlockStore> Node<B>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    pub fn new(config: NodeConfig<B>) -> Self {
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        println!("Local peer id: {:?}", local_peer_id);

        let transport = build_transport(local_key.clone());

        let store = Arc::new(config.blockstore);
        // temp behaviour to be replaced with graphsync
        // let behaviour = Ping::new(PingConfig::new().with_keep_alive(true));

        let behaviour = DataTransfer::new(
            local_peer_id,
            Graphsync::new(GraphsyncConfig::default(), store.clone()),
            PeerDiscovery::new(PeerDiscoveryConfig::default(), local_peer_id),
        );



        let mut swarm = SwarmBuilder::new(transport, behaviour, local_peer_id)
            .executor(Box::new(|fut| {
                wasm_bindgen_futures::spawn_local(fut);
            }))
            .build();

        if let Some(maddr) = config.listening_multiaddr {
            Swarm::listen_on(&mut swarm, maddr).unwrap();
        }

        Node {
            swarm,
            store: { store.clone() },
        }
    }

    pub async fn run(mut self) {
        loop {
            match self.swarm.select_next_some().await {
                SwarmEvent::NewListenAddr { address, .. } => println!("Listening on {:?}", address),
                SwarmEvent::Behaviour(event) => println!("{:?}", event),
                _ => {}
            }
        }
    }

    pub async fn run_request(&mut self, addr: Multiaddr, peer: PeerId, cid: Cid) {
        self.swarm.behaviour_mut().add_address(&peer, addr);

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let params = DealParams {
            selector: Some(selector.clone()),
            ..Default::default()
        };

        let start = Instant::now();

        if let Err(e) = self.swarm.behaviour_mut().pull(peer, cid, selector, params) {
            panic!("transfer failed {}", e);
        }

        log::info!("==> Started transfer");

        loop {
            let ev = self.swarm.next().await.unwrap();
            if let SwarmEvent::Behaviour(event) = ev {
                match event {
                    DataTransferEvent::Completed(_, Ok(())) => {
                        let elapsed = start.elapsed();
                        log::info!("transfer took {:?}", elapsed);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn fill_random_data(&self) {
        let mut data = vec![0u8; DATA_SIZE];
        rand::thread_rng().fill_bytes(&mut data);
        let root = dag_service::add(self.store.clone(), &data).unwrap();
        println!("added data {:?}", root);
    }
}
