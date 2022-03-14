use super::network::MessageCodec;
use super::traversal::{AsyncLoader, Error, Progress, Selector};
use super::{
    Extensions, GraphsyncCodec, GraphsyncMessage, GraphsyncProtocol, GraphsyncRequest, Prefix,
    RequestId,
};
use async_std::channel::Sender;
use blockstore::types::BlockStore;
use futures::{channel::mpsc, prelude::*};
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Block, Cid, Ipld};
use libp2p::{
    core::muxing::{
        event_from_ref_and_wrap, outbound_from_ref_and_wrap, StreamMuxer, StreamMuxerBox,
    },
    core::transport::{Boxed, Transport},
    core::upgrade,
    Multiaddr, PeerId,
};
use multistream_select::{dialer_select_proto, listener_select_proto};
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc,
};

#[cfg(not(target_os = "unknown"))]
use async_std::task::spawn;

#[cfg(target_os = "unknown")]
use async_std::task::spawn_local as spawn;

#[derive(Debug)]
pub enum RequestEvent {
    Progress {
        req_id: RequestId,
        link: Cid,
        data: Ipld,
        size: usize,
    },
    Completed(RequestId, Result<(), Error>),
}

#[derive(Clone)]
pub struct Client<S: BlockStore> {
    store: Arc<S>,
    id_counter: Arc<AtomicI32>,
    transport: Boxed<(PeerId, StreamMuxerBox)>,
}

impl<S: 'static + BlockStore> Client<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>, tp: Boxed<(PeerId, StreamMuxerBox)>) -> Self {
        Self {
            store,
            transport: tp,
            id_counter: Arc::new(AtomicI32::new(1)),
        }
    }
    pub async fn dial(&self, maddr: Multiaddr) -> Result<Arc<StreamMuxerBox>, String> {
        let (_peer, mux) = self
            .transport
            .clone()
            .dial(maddr)
            .map_err(|e| e.to_string())?
            .await
            .map_err(|e| e.to_string())?;
        Ok(Arc::new(mux))
    }
    pub async fn request(
        self,
        mux: Arc<StreamMuxerBox>,
        root: Cid,
        sel: Selector,
        ext: Extensions,
    ) -> Result<mpsc::Receiver<RequestEvent>, String> {
        let outbound = outbound_from_ref_and_wrap(mux.clone())
            .await
            .map_err(|e| e.to_string())?;

        let protos = vec![b"/ipfs/graphsync/1.0.0"];
        let (_proto, mut io) = dialer_select_proto(
            outbound,
            protos.into_iter(),
            multistream_select::Version::V1,
        )
        .await
        .map_err(|e| e.to_string())?;

        let mut codec = GraphsyncCodec::<S::Params>::default();

        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let mut msg = GraphsyncMessage::default();
        msg.requests.insert(
            id,
            GraphsyncRequest {
                id,
                root,
                selector: sel.clone(),
                extensions: ext,
            },
        );

        codec
            .write_message(&GraphsyncProtocol, &mut io, msg)
            .await
            .map_err(|e| e.to_string())?;
        io.close().await.map_err(|e| e.to_string())?;

        let (s, r) = mpsc::channel(64);
        let mut ls = s.clone();
        let loader = AsyncLoader::new(self.store.clone(), move |blk| {
            ls.start_send(RequestEvent::Progress {
                req_id: id,
                link: blk.link,
                size: blk.size,
                data: blk.data,
            })
            .unwrap();
            Ok(())
        });
        let sender = loader.sender();
        let ic = mux.clone();
        spawn(async move {
            if let Err(e) = self.msg_loop(ic, sender).await {
                println!("error {:?}", e);
            }
        });
        let mut ps = s.clone();
        spawn(async move {
            let mut progress = Progress::new(loader);
            let result = progress
                .walk_adv(&Ipld::Link(root), sel, &|_, _| Ok(()))
                .await;
            ps.start_send(RequestEvent::Completed(id, result)).unwrap();
        });
        Ok(r)
    }

    async fn msg_loop<M>(
        self,
        muxer: M,
        sender: Arc<Sender<Block<S::Params>>>,
    ) -> Result<(), String>
    where
        M: Deref + Clone + Send,
        M::Target: StreamMuxer,
        <<M as Deref>::Target as StreamMuxer>::Error: std::fmt::Display + Send,
        <<M as Deref>::Target as StreamMuxer>::Substream: Send,
    {
        let inbound = loop {
            if let Some(s) = event_from_ref_and_wrap(muxer.clone())
                .await
                .map_err(|e| e.to_string())?
                .into_inbound_substream()
            {
                break s;
            }
        };
        let protos = vec![b"/ipfs/graphsync/1.0.0"];
        let (_proto, mut io) = listener_select_proto(inbound, protos.into_iter())
            .await
            .map_err(|e| e.to_string())?;
        let mut codec = GraphsyncCodec::<S::Params>::default();
        while let Ok(msg) = codec.read_message(&GraphsyncProtocol, &mut io).await {
            let blocks: Vec<Block<S::Params>> = msg
                .blocks
                .iter()
                .map_while(|(prefix, data)| {
                    let prefix = Prefix::new_from_bytes(prefix).ok()?;
                    let cid = prefix.to_cid(data).ok()?;
                    Some(Block::new_unchecked(cid, data.to_vec()))
                })
                .collect();

            for block in blocks {
                sender.try_send(block).unwrap();
            }
        }
        io.close().await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::traversal::{RecursionLimit, Selector};
    use super::super::{Config, Graphsync};
    use super::*;
    use async_std::task;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use blockstore::types::DBStore;
    use futures::prelude::*;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::multihash::Code;
    use libipld::{Block, Cid};
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::multiaddr;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::{mplex, PeerId, Swarm, Transport};
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
        store: Arc<MemoryBlockStore>,
        swarm: Swarm<Graphsync<MemoryBlockStore>>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let store = Arc::new(MemoryBlockStore::default());
            let mut swarm = Swarm::new(
                trans,
                Graphsync::new(Config::default(), store.clone()),
                peer_id,
            );
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
                store,
            }
        }

        fn swarm(&mut self) -> &mut Swarm<Graphsync<MemoryBlockStore>> {
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
    }

    #[async_std::test]
    async fn test_client() {
        let peer1 = Peer::new();

        let store = peer1.store.clone();
        let maddr1 = peer1.addr.clone();
        let peer1 = peer1.spawn("peer1");

        let leaf1 = ipld!({ "name": "leaf1", "size": 12 });
        let leaf1_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf1).unwrap();
        store.insert(&leaf1_block).unwrap();

        let leaf2 = ipld!({ "name": "leaf2", "size": 6 });
        let leaf2_block = Block::encode(DagCborCodec, Code::Sha2_256, &leaf2).unwrap();
        store.insert(&leaf2_block).unwrap();

        let parent = ipld!({
            "children": [leaf1_block.cid(), leaf2_block.cid()],
            "favouriteChild": leaf2_block.cid(),
            "name": "parent",
        });
        let parent_block = Block::encode(DagCborCodec, Code::Sha2_256, &parent).unwrap();
        store.insert(&parent_block).unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let (peer2, trans) = mk_transport();
        let store2 = Arc::new(MemoryBlockStore::default());

        let client = Client::new(store2, trans);

        let maddr1 = maddr1.with(multiaddr::Protocol::P2p(peer1.into()));

        let mux = client.dial(maddr1).await.unwrap();

        let mut stream = client
            .request(mux, *parent_block.cid(), selector, Default::default())
            .await
            .unwrap();
        while let Some(evt) = stream.next().await {
            match evt {
                RequestEvent::Completed(_, res) => {
                    assert_eq!(res, Ok(()));
                }
                RequestEvent::Progress { .. } => {}
            }
        }
    }
}
