use async_std::task;
use blockstore::memory::MemoryDB;
use criterion::async_executor::FuturesExecutor;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main, BatchSize, Throughput};
use dag_service::add;
use data_transfer::{Dt, DtOptions, DtParams};
use futures::prelude::*;
use libipld::Cid;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::tcp::TcpConfig;
use libp2p::{identity, mplex, multiaddr, Multiaddr};
use libp2p::{PeerId, Transport};
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
        group.bench_with_input(BenchmarkId::new("stream", size), size, move |b, &size| {
            b.iter_batched(
                || prepare_blocks(size),
                |dag| async move {
                    let (peer1, trans) = mk_transport();
                    let mut provider = Dt::new(
                        peer1.clone(),
                        dag.store,
                        trans,
                        DtOptions::default().as_listener("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
                    );

                    let mut addresses = provider.addresses().await.unwrap();

                    let (peer2, trans) = mk_transport();
                    let store = Arc::new(MemoryDB::default());

                    let client = Dt::new(peer2, store, trans, Default::default());

                    let maddr1 = addresses.pop().unwrap();

                    let stream = client
                        .pull(peer1, maddr1, DtParams::full_from_root(dag.root))
                        .await
                        .unwrap();
                    stream.discard_read().await.unwrap();
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
