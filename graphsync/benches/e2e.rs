use async_std::task;
use blockstore::memory::MemoryDB;
use criterion::async_executor::FuturesExecutor;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main, BatchSize, Throughput};
use dag_service::{add, cat};
use futures::prelude::*;
use graphsync::{
    traversal::{BlockIterator, Progress, RecursionLimit, Selector, StoreLoader},
    Graphsync, GraphsyncEvent,
};
use libipld::{Cid, Ipld};
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::swarm::SwarmEvent;
use libp2p::tcp::TcpConfig;
use libp2p::yamux::YamuxConfig;
use libp2p::{identity, Multiaddr};
use libp2p::{PeerId, Swarm, Transport};
use pprof::criterion::{Output, PProfProfiler};
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
        .multiplex(YamuxConfig::default())
        .timeout(Duration::from_secs(20))
        .boxed();
    (peer_id, transport)
}

struct Peer {
    peer_id: PeerId,
    addr: Multiaddr,
    swarm: Swarm<Graphsync<MemoryDB>>,
}

impl Peer {
    fn new(store: Arc<MemoryDB>) -> Self {
        let (peer_id, trans) = mk_transport();
        let mut swarm = Swarm::new(
            trans,
            Graphsync::new(Default::default(), store.clone()),
            peer_id,
        );
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

    fn swarm(&mut self) -> &mut Swarm<Graphsync<MemoryDB>> {
        &mut self.swarm
    }

    fn spawn(mut self, _name: &'static str) -> PeerId {
        let peer_id = self.peer_id;
        task::spawn(async move {
            loop {
                let event = self.swarm.next().await;
            }
        });
        peer_id
    }

    async fn next(&mut self) -> Option<GraphsyncEvent> {
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

    let mut group = c.benchmark_group("from_dag_size");
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

                    let id = peer2.swarm().behaviour_mut().request(
                        peer1,
                        dag.root,
                        selector,
                        Default::default(),
                    );

                    loop {
                        if let Some(event) = peer2.next().await {
                            match event {
                                GraphsyncEvent::Complete(rid, Ok(())) => {
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
    }
}

fn bench_traversal(c: &mut Criterion) {
    static MB: usize = 1024 * 1024;

    let mut group = c.benchmark_group("traversal");
    for size in [4 * MB, 10 * MB].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        // group.bench_with_input(BenchmarkId::new("callback", size), size, move |b, &size| {
        //     b.iter_batched(
        //         || prepare_blocks(size),
        //         |dag| async move {
        //             let store = Arc::<MemoryDB>::try_unwrap(dag.store).unwrap();
        //             let loader = StoreLoader { store };
        //             let mut progress = Progress {
        //                 loader,
        //                 path: Default::default(),
        //                 last_block: None,
        //             };
        //             let selector = Selector::ExploreRecursive {
        //                 limit: RecursionLimit::None,
        //                 sequence: Box::new(Selector::ExploreAll {
        //                     next: Box::new(Selector::ExploreRecursiveEdge),
        //                 }),
        //                 current: None,
        //             };

        //             progress
        //                 .walk_adv(&Ipld::Link(dag.root), selector, &|_, _| Ok(()))
        //                 .await
        //                 .unwrap();
        //         },
        //         BatchSize::SmallInput,
        //     );
        // });
        group.bench_with_input(BenchmarkId::new("iterator", size), size, move |b, &size| {
            b.iter_batched(
                || prepare_blocks(size),
                |dag| {
                    let selector = Selector::ExploreRecursive {
                        limit: RecursionLimit::None,
                        sequence: Box::new(Selector::ExploreAll {
                            next: Box::new(Selector::ExploreRecursiveEdge),
                        }),
                        current: None,
                    };

                    let it =
                        BlockIterator::new(dag.store, dag.root, selector).ignore_duplicate_links();
                    for _ in it {}
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
criterion_group!(benches, bench_traversal);
criterion_main!(benches);
