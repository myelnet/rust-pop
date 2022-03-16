use async_std::task;
use blockstore::memory::MemoryDB;
use criterion::async_executor::FuturesExecutor;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main, BatchSize, Throughput};
use dag_service::add;
use data_transfer::{client::Client, DataTransferBehaviour, DataTransferEvent};
use futures::prelude::*;
use graphsync::{
    traversal::{RecursionLimit, Selector},
    Graphsync,
};
use libipld::Cid;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::SwarmEvent;
use libp2p::tcp::TcpConfig;
use libp2p::{identity, mplex, multiaddr, Multiaddr};
use libp2p::{PeerId, Swarm, Transport};
use rand::prelude::*;
use std::sync::Arc;
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
        .multiplex(mplex::MplexConfig::new())
        .timeout(Duration::from_secs(20))
        .boxed();
    (peer_id, transport)
}

struct Peer {
    peer_id: PeerId,
    addr: Multiaddr,
    swarm: Swarm<DataTransferBehaviour<MemoryDB>>,
}

impl Peer {
    fn new(store: Arc<MemoryDB>) -> Self {
        let (peer_id, trans) = mk_transport();
        let gs = Graphsync::new(Default::default(), store.clone());
        let dt = DataTransferBehaviour::new(peer_id, gs);
        let mut swarm = Swarm::new(trans, dt, peer_id);
        Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
        while swarm.next().now_or_never().is_some() {}
        let addr = Swarm::listeners(&swarm).next().unwrap().clone();
        Self {
            peer_id,
            addr,
            swarm,
        }
    }

    fn add_address(&mut self, peer: &Peer) {
        self.swarm
            .behaviour_mut()
            .add_address(&peer.peer_id, peer.addr.clone());
    }

    fn swarm(&mut self) -> &mut Swarm<DataTransferBehaviour<MemoryDB>> {
        &mut self.swarm
    }

    fn spawn(mut self, _name: &'static str) -> PeerId {
        let peer_id = self.peer_id;
        task::spawn(async move {
            loop {
                let _ = self.swarm.next().await;
            }
        });
        peer_id
    }

    async fn next(&mut self) -> Option<DataTransferEvent> {
        loop {
            let ev = self.swarm.next().await?;
            if let SwarmEvent::Behaviour(event) = ev {
                return Some(event);
            }
        }
    }
}

fn prepare_blocks(size: usize) -> Dag {
    let mut data = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut data);

    let store = Arc::new(MemoryDB::default());

    let root = add(store.clone(), &data).unwrap();
    Dag {
        store,
        root: root.unwrap(),
    }
}

struct Dag {
    root: Cid,
    store: Arc<MemoryDB>,
}

fn bench_transfer(c: &mut Criterion) {
    static MB: usize = 1024 * 1024;

    let mut group = c.benchmark_group("data_transfer");
    for size in [MB, 4 * MB, 15 * MB, 60 * MB].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("async", size), size, move |b, &size| {
            b.iter_batched(
                || prepare_blocks(size),
                |dag| async move {
                    let peer1 = Peer::new(dag.store);
                    let mut peer2 = Peer::new(Arc::new(MemoryDB::default()));
                    peer2.add_address(&peer1);

                    let peer1 = peer1.spawn("peer1");

                    let selector = Selector::ExploreRecursive {
                        limit: RecursionLimit::None,
                        sequence: Box::new(Selector::ExploreAll {
                            next: Box::new(Selector::ExploreRecursiveEdge),
                        }),
                        current: None,
                    };

                    let _ = peer2
                        .swarm()
                        .behaviour_mut()
                        .pull(peer1, dag.root, selector, Default::default())
                        .unwrap();

                    loop {
                        if let Some(event) = peer2.next().await {
                            match event {
                                DataTransferEvent::Completed(_chid, Ok(())) => {
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_with_input(BenchmarkId::new("client", size), size, move |b, &size| {
            b.iter_batched(
                || prepare_blocks(size),
                |dag| async move {
                    let peer1 = Peer::new(dag.store);
                    let maddr1 = peer1.addr.clone();
                    let peer1 = peer1.spawn("peer1");

                    let (peer2, trans) = mk_transport();
                    let store = Arc::new(MemoryDB::default());

                    let client = Client::new(store, trans);

                    let selector = Selector::ExploreRecursive {
                        limit: RecursionLimit::None,
                        sequence: Box::new(Selector::ExploreAll {
                            next: Box::new(Selector::ExploreRecursiveEdge),
                        }),
                        current: None,
                    };

                    let maddr1 = maddr1.with(multiaddr::Protocol::P2p(peer1.into()));

                    let stream = client
                        .pull(peer1, maddr1, dag.root, selector, Default::default())
                        .await
                        .unwrap();
                    let mut reader = stream.reader();

                    let mut output: Vec<u8> = Vec::new();
                    loop {
                        if let Ok(_) = reader.read(&mut output).await {
                            output = Vec::new();
                        } else {
                            break;
                        }
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
}

// criterion_group! {
//     name = benches;
//     config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
//     targets = bench_traversal
// }
criterion_group!(benches, bench_transfer);
criterion_main!(benches);
