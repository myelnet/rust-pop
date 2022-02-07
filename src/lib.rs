#[cfg(feature = "browser")]
mod browser;

mod dag_service;
mod empty_map;
mod graphsync;
mod graphsync_pb;
mod network;
mod request_manager;
mod response_manager;
mod traversal;

use crate::graphsync::{Config as GraphsyncConfig, Graphsync, GraphsyncEvent};
use crate::traversal::{RecursionLimit, Selector};
use libipld::mem::MemStore;
use libipld::Cid;
use libipld::DefaultParams;
use libp2p::futures::StreamExt;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::wasm_ext;
use libp2p::{
    self, core, core::muxing::StreamMuxerBox, core::transport::Boxed,
    core::transport::OptionalTransport, identity, mplex, noise, yamux, Multiaddr, PeerId,
    Transport,
};
use rand::prelude::*;
use std::sync::{Arc};
use std::time::{Duration, Instant};

#[cfg(not(target_os = "unknown"))]
use libp2p::{dns, tcp, websocket};

#[cfg(not(target_os = "unknown"))]
use async_std::task;

const DATA_SIZE: usize = 104857600;
pub struct Node {
    pub swarm: Swarm<Graphsync<MemStore<DefaultParams>>>,
    pub store: Arc<MemStore<DefaultParams>>,
}

#[derive(Debug)]
pub struct NodeConfig {
    pub listening_multiaddr: Multiaddr,
    pub wasm_external_transport: Option<wasm_ext::ExtTransport>,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        println!("Local peer id: {:?}", local_peer_id);

        let transport = build_transport(config.wasm_external_transport, local_key.clone());

        let store = Arc::new(MemStore::<DefaultParams>::default());
        // temp behaviour to be replaced with graphsync
        // let behaviour = Ping::new(PingConfig::new().with_keep_alive(true));
        let behaviour = Graphsync::new(GraphsyncConfig::default(), store.clone());

        let mut swarm = Swarm::new(transport, behaviour, local_peer_id);

        // Listen on all interfaces and whatever port the OS assigns.  Websockt can't receive incoming connections
        // on browser
        #[cfg(not(target_os = "unknown"))]
        Swarm::listen_on(&mut swarm, config.listening_multiaddr).unwrap();

        Node {
            swarm,
            store: { store.clone() },
        }
    }

    pub async fn run(&mut self) {
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

        let start = Instant::now();

        self.swarm.behaviour_mut().request(peer, cid, selector);

        loop {
            let ev = self.swarm.next().await.unwrap();
            if let SwarmEvent::Behaviour(event) = ev {
                if let GraphsyncEvent::Complete(_, Ok(())) = event {
                    let elapsed = start.elapsed();
                    println!("transfer took {:?}", elapsed);
                    break;
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

/// Builds the transport stack that LibP2P will communicate over.
pub fn build_transport(
    wasm_external_transport: Option<wasm_ext::ExtTransport>,
    local_key: identity::Keypair,
) -> Boxed<(PeerId, StreamMuxerBox)> {
    // Can have no external transport if not compiling for wasm
    let transport = if let Some(t) = wasm_external_transport {
        OptionalTransport::some(t)
    } else {
        OptionalTransport::none()
    };

    // if not compiling for wasm
    #[cfg(not(target_os = "unknown"))]
    let transport = transport.or_transport({
        let desktop_trans = tcp::TcpConfig::new().nodelay(true);
        let desktop_trans =
            websocket::WsConfig::new(desktop_trans.clone()).or_transport(desktop_trans);
        OptionalTransport::some(
            if let Ok(dns) = task::block_on(dns::DnsConfig::system(desktop_trans.clone())) {
                dns.boxed()
            } else {
                desktop_trans.map_err(dns::DnsErr::Transport).boxed()
            },
        )
    });

    // let transport = libp2p::tcp::TcpConfig::new().nodelay(true);
    // let transport = libp2p::websocket::WsConfig::new(transport.clone()).or_transport(transport);
    // let transport = async_std::task::block_on(libp2p::dns::DnsConfig::system(transport)).unwrap();
    let auth_config = {
        let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&local_key)
            .expect("Noise key generation failed");

        noise::NoiseConfig::xx(dh_keys).into_authenticated()
    };

    let mplex_config = {
        let mut mplex_config = mplex::MplexConfig::new();
        mplex_config.set_max_buffer_size(usize::MAX);

        let mut yamux_config = yamux::YamuxConfig::default();
        yamux_config.set_max_buffer_size(16 * 1024 * 1024);
        yamux_config.set_receive_window_size(16 * 1024 * 1024);
        core::upgrade::SelectUpgrade::new(yamux_config, mplex_config)
    };

    transport
        .upgrade(core::upgrade::Version::V1)
        .authenticate(auth_config)
        .multiplex(mplex_config)
        .timeout(Duration::from_secs(20))
        .boxed()
}
