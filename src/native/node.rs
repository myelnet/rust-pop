use crate::native::server::{
    build_transport, export_file, handle_rejection, read_file, retrieve_file,
};
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
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{self, identity, Multiaddr, PeerId};
use parking_lot::Mutex;
use rand::prelude::*;
use routing::{Config as PeerDiscoveryConfig, PeerDiscovery};
use std::collections::HashMap;
use std::sync::Arc;
use warp::Filter;

const DATA_SIZE: usize = 104857600;

pub struct Node<B: 'static + BlockStore>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    pub swarm: Arc<Mutex<Swarm<DataTransfer<B>>>>,
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

        let behaviour = DataTransfer::new(
            local_peer_id,
            Graphsync::new(GraphsyncConfig::default(), store.clone()),
            PeerDiscovery::new(PeerDiscoveryConfig::default(), local_peer_id),
        );

        let mut swarm = Swarm::new(transport, behaviour, local_peer_id);

        if let Some(maddr) = config.listening_multiaddr {
            Swarm::listen_on(&mut swarm, maddr).unwrap();
        }

        Node {
            swarm: Arc::new(Mutex::new(swarm)),
            store: { store.clone() },
        }
    }

    pub async fn run(&mut self) {
        self.start_server().await;
        loop {
            let mut swarm = self.swarm.lock();
            let ev = swarm.select_next_some();
            // This method must only be called if the current thread logically owns a MutexGuard
            // but that guard has to be discarded using mem::forget. !!!!! This is the case here.
            //  frees the lock so other processes can act on the swarm whilst the event future awaits
            // this also only applies to native builds which have operations being input via cli
            unsafe {
                self.swarm.force_unlock();
            }

            match ev.await {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening on {:?}", address)
                }
                SwarmEvent::Behaviour(event) => println!("{:?}", event),
                _ => {}
            }
        }
    }

    pub async fn start_server(&mut self) {
        let swarm = self.swarm.clone();
        let store = self.store.clone();
        let store_filter = warp::any().map(move || store.clone());
        let swarm_filter = warp::any().map(move || swarm.clone());

        let add_file = warp::post()
            .and(warp::path("add"))
            .and(warp::body::bytes())
            .map(|bytes: warp::hyper::body::Bytes| {
                return std::str::from_utf8(&bytes).unwrap().to_string();
            })
            .and(store_filter.clone())
            .and_then(|path: String, store: Arc<B>| {
                return read_file(path.clone(), store.clone());
            });

        let export_file = warp::post()
            .and(warp::path("export"))
            .and(warp::body::json())
            .and(store_filter.clone())
            .and_then(|simple_map: HashMap<String, String>, store: Arc<B>| {
                //  can safely unwrap entries as if they are None the method will just return a failure
                //  response to the requesting client
                return export_file(
                    simple_map.get("cid").unwrap().to_string(),
                    simple_map.get("path").unwrap().to_string(),
                    store.clone(),
                );
            });

        let retrieve_file = warp::post()
            .and(warp::path("retrieve"))
            .and(warp::body::json())
            .and(swarm_filter.clone())
            .and_then(
                |simple_map: HashMap<String, String>,
                 behaviour: Arc<Mutex<Swarm<DataTransfer<B>>>>| {
                    //  can safely unwrap entries as if they are None the method will just return a failure
                    //  response to the requesting client
                    return retrieve_file(
                        simple_map.get("cid").unwrap().to_string(),
                        simple_map.get("peer").unwrap().to_string(),
                        simple_map.get("multiaddr").unwrap().to_string(),
                        behaviour,
                    );
                },
            );

        let routes = add_file
            .or(export_file)
            .or(retrieve_file)
            .recover(handle_rejection);
        // serve on port 27403
        async_std::task::spawn(
            async move { warp::serve(routes).run(([127, 0, 0, 1], 27403)).await },
        );
    }

    pub async fn run_request(&mut self, addr: Multiaddr, peer: PeerId, cid: Cid) {
        self.swarm.lock().behaviour_mut().add_address(&peer, addr);

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

        if let Err(e) = self
            .swarm
            .lock()
            .behaviour_mut()
            .pull(peer, cid, selector, params)
        {
            panic!("transfer failed {}", e);
        }

        log::info!("==> Started transfer");

        loop {
            let ev = self.swarm.lock().next().await.unwrap();
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
