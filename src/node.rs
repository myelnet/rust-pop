use crate::graphsync::{Config as GraphsyncConfig, Graphsync};
use libipld::mem::MemStore;
use libipld::DefaultParams;
use libp2p::futures::StreamExt;
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{
    core, core::muxing::StreamMuxerBox, core::transport::Boxed, identity, mplex, noise, yamux,
    Multiaddr, PeerId, Transport,
};
use std::time::Duration;

pub struct Node {
    swarm: Swarm<Graphsync<MemStore<DefaultParams>>>,
}

#[derive(Debug)]
pub struct NodeConfig {
    pub listening_multiaddr: Multiaddr,
}

impl Node {
    pub fn new(config: NodeConfig) -> Self {
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        println!("Local peer id: {:?}", local_peer_id);

        let transport = build_transport(local_key.clone());

        let store = MemStore::<DefaultParams>::default();
        // temp behaviour to be replaced with graphsync
        // let behaviour = Ping::new(PingConfig::new().with_keep_alive(true));
        let behaviour = Graphsync::new(GraphsyncConfig::default(), store);

        let mut swarm = Swarm::new(transport, behaviour, local_peer_id);

        Swarm::listen_on(&mut swarm, config.listening_multiaddr).unwrap();

        Node { swarm }
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
}

/// Builds the transport stack that LibP2P will communicate over.
pub fn build_transport(local_key: identity::Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let transport = libp2p::tcp::TcpConfig::new().nodelay(true);
    let transport = libp2p::websocket::WsConfig::new(transport.clone()).or_transport(transport);
    let transport = async_std::task::block_on(libp2p::dns::DnsConfig::system(transport)).unwrap();
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
