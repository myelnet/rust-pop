use super::fsm::{Channel, ChannelEvent as FsmEvent};
use super::mimesniff::{detect_content_type, ContentType};
use super::network::{
    DealProposal, DealResponse, DealStatus, MessageType, PullParams, TransferMessage,
    TransferRequest, EXTENSION_KEY,
};
use blockstore::types::BlockStore;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use futures::{
    channel::{mpsc, oneshot},
    join,
    prelude::*,
    select,
    stream::{FuturesUnordered, IntoAsyncRead, TryStreamExt},
};
use graphsync::network::MessageCodec;
use graphsync::traversal::{resolve_unixfs, AsyncLoader, Error, Progress, Selector};
use graphsync::{
    Extensions, GraphsyncCodec, GraphsyncMessage, GraphsyncProtocol, GraphsyncRequest, Prefix,
    RequestId,
};
use libipld::codec::Decode;
use libipld::store::{DefaultParams, StoreParams};
use libipld::{Block, Cid, Ipld};
use libp2p::{
    core::muxing::{event_from_ref_and_wrap, outbound_from_ref_and_wrap, StreamMuxerBox},
    core::transport::{Boxed, Transport},
    core::ProtocolName,
    Multiaddr, PeerId,
};
use multistream_select::{dialer_select_proto, listener_select_proto};
use smallvec::SmallVec;
use std::collections::hash_map::{Entry, HashMap};
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc,
};

#[cfg(not(target_os = "unknown"))]
use async_std::task::spawn;

#[cfg(target_os = "unknown")]
use async_std::task::spawn_local as spawn;

pub struct PullStream {
    pub content_type: ContentType,
    pub channel: mpsc::Receiver<Result<Vec<u8>, std::io::Error>>,
}

impl PullStream {
    pub fn reader(self) -> IntoAsyncRead<mpsc::Receiver<Result<Vec<u8>, std::io::Error>>> {
        self.channel.into_async_read()
    }
}

#[derive(Clone)]
pub struct Client<S: BlockStore> {
    store: Arc<S>,
    id_counter: Arc<AtomicI32>,
    outbound_events: mpsc::Sender<NetEvent>,
}

impl<S: 'static + BlockStore> Client<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(store: Arc<S>, tp: Boxed<(PeerId, StreamMuxerBox)>) -> Self {
        let (s, r) = mpsc::channel(128);
        let transport = tp.clone();
        spawn(async move {
            if let Err(e) = conn_loop(r, transport).await {
                println!("connection loop: {:?}", e);
            }
        });
        Self {
            store,
            id_counter: Arc::new(AtomicI32::new(1)),
            outbound_events: s,
        }
    }
    pub async fn pull(
        &self,
        peer: PeerId,
        maddr: Multiaddr,
        root: Cid,
        sel: Selector,
        params: PullParams,
    ) -> Result<PullStream, String> {
        // For now we simplify and use a single id for all protocol requests
        // it will be updated in any case when switching over to UUIDs.
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);

        let cid = CidCbor::from(root);
        let voucher = DealProposal::Pull {
            id: id as u64,
            payload_cid: cid.clone(),
            params,
        };
        let message = TransferMessage {
            is_rq: true,
            request: Some(TransferRequest {
                root: cid,
                mtype: MessageType::New,
                pause: false,
                partial: false,
                pull: true,
                selector: sel.clone(),
                voucher_type: voucher.voucher_type(),
                voucher: Some(voucher),
                transfer_id: id as u64,
                restart_channel: Default::default(),
            }),
            response: None,
        };
        let buf = message.marshal_cbor().map_err(|e| e.to_string())?;
        let mut extensions = HashMap::new();
        extensions.insert(EXTENSION_KEY.to_string(), buf);

        let mut msg = GraphsyncMessage::default();
        msg.requests.insert(
            id,
            GraphsyncRequest {
                id,
                root,
                selector: sel.clone(),
                extensions,
            },
        );
        // This channel will receive inbound messages from all protocols
        let (inbound_send, mut inbound_receive) = mpsc::channel(64);

        // outbound events are sent to the connection loop to be streamed over the relevant
        // protocols.
        let mut outbound = self.outbound_events.clone();
        outbound
            .start_send(NetEvent::NewRequest {
                id,
                peer,
                maddr,
                msg: NetMsg::Graphsync(msg),
                chan: inbound_send,
            })
            .map_err(|e| e.to_string())?;

        // This is the final channel that will consume the transfer data.
        let (mut s, r) = mpsc::channel(64);
        // This channel waits for the first bytes to come in.
        let (os, or) = oneshot::channel();

        let store = self.store.clone();
        spawn(async move {
            let mut state = Channel::New {
                id: Default::default(),
                deal_id: id as u64,
            };

            let (mut bs, br) = mpsc::channel(16);
            let mut loader_sender = bs.clone();
            let loader = AsyncLoader::new(store, move |blk| {
                loader_sender.start_send(blk).unwrap();
                Ok(())
            });
            // This channel receives graphsync blocks to be sent to the async loader.
            let sender = loader.sender();

            let mut progress = Progress::new(loader);
            let node = Ipld::Link(root);

            // content type sender is take once when receiving the first bytes
            let mut ct_sender = Some(os);
            let mut ts = s.clone();
            // There is a race between the traversal future and when the block stream finishes
            // so we must wait for both to end before we can register all the blocks are received.
            let mut traverse = Box::pin(async {
                join!(
                    progress
                        .walk_adv(&node, sel, &|_, _| Ok(()))
                        .then(|res| async move {
                            // once the traversal is over we close the block sender channel
                            // to let the block stream fold complete.
                            bs.close_channel();
                            res
                        }),
                    // the block receiver is folded to capture the total size of content received.
                    br.fold(0, |acc, blk| {
                        // during the fold operation we're actually resolving the content bytes and
                        // sending them over to the stream.
                        if let Some(Ipld::Bytes(bytes)) =
                            resolve_unixfs(&blk.data).or(Some(blk.data))
                        {
                            if let Some(os) = ct_sender.take() {
                                drop(os.send(detect_content_type(&bytes)));
                            }
                            ts.start_send(Ok(bytes)).unwrap();
                        }
                        future::ready(acc + blk.size)
                    })
                )
            })
            .fuse();

            loop {
                // select the first future to complete after each iteration. Once we've both
                // received all the blocks and the completion message we break out of it.
                select! {
                    (_result, total) = traverse => {
                        // for now we register a single block received event at the end. This may changed in
                        // the future to process payment operations.
                        state = state.transition(FsmEvent::BlockReceived { size: total });
                        state = state.transition(FsmEvent::AllBlocksReceived);
                         if let Channel::Completed { .. } = state {
                             s.close_channel();
                             outbound.start_send(NetEvent::Cleanup { id, peer }).unwrap();
                             break;
                         }
                    },
                    // protocol inbound messages are received first.
                    ev = inbound_receive.select_next_some() => {
                        match ev {
                        NetMsg::Graphsync(msg) => {
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
                        NetMsg::Transfer(msg) => {
                            // pull request clients should not receive requests
                            assert!(!msg.is_rq);
                            if let Some(DealResponse::Pull {
                                status: _deal_status @ DealStatus::Completed,
                                ..
                            }) = msg.response.expect("to be a response").voucher
                            {
                                // check the last transition to see if we can clean up.
                                state = state.transition(FsmEvent::Completed);
                                if let Channel::Completed { .. } = state {
                                    s.close_channel();
                                    outbound.start_send(NetEvent::Cleanup { id, peer }).unwrap();
                                    break;
                                }
                            }
                        }
                        }
                    },
                };
            }
        });
        // Wait to receive the content type before returning the stream
        let content_type = or.await.map_err(|e| e.to_string())?;
        Ok(PullStream {
            content_type,
            channel: r,
        })
    }
}

enum NetMsg {
    Graphsync(GraphsyncMessage),
    Transfer(TransferMessage),
}

impl ProtocolName for NetMsg {
    fn protocol_name(&self) -> &[u8] {
        match *self {
            NetMsg::Graphsync(_) => b"/ipfs/graphsync/1.0.0",
            NetMsg::Transfer(_) => b"/fil/datatransfer/1.2.0",
        }
    }
}

enum NetEvent {
    NewRequest {
        id: RequestId,
        peer: PeerId,
        maddr: Multiaddr,
        msg: NetMsg,
        chan: mpsc::Sender<NetMsg>,
    },
    Message {
        peer: PeerId,
        msg: NetMsg,
    },
    Cleanup {
        id: RequestId,
        peer: PeerId,
    },
}

enum ChannelEvent {
    Open {
        id: RequestId,
        chan: mpsc::Sender<NetMsg>,
    },
    Close(RequestId),
}

// The connection loop is responsible for opening new streams with peer or sending to open
// connections. The request contains a sender to collect inbound messages.
async fn conn_loop(
    mut events: mpsc::Receiver<NetEvent>,
    transport: Boxed<(PeerId, StreamMuxerBox)>,
) -> Result<(), String> {
    // here we maintain a list of open connections with peers. If a peer isn't included in this
    // list we dial and open a new stream. Channel sends outbound messages.
    let mut outbound: HashMap<PeerId, mpsc::Sender<NetMsg>> = HashMap::new();

    // here we maintain a list of channels listening for inbound messages for a given peer.
    let mut inbound: HashMap<PeerId, mpsc::Sender<ChannelEvent>> = HashMap::new();

    while let Some(ev) = events.next().await {
        match ev {
            NetEvent::NewRequest {
                id,
                peer,
                maddr,
                msg,
                chan,
            } => {
                match (inbound.entry(peer), outbound.entry(peer)) {
                    (Entry::Occupied(mut in_entry), Entry::Occupied(mut out_entry)) => {
                        // We already have an open connection from a previous transfer
                        in_entry
                            .get_mut()
                            .start_send(ChannelEvent::Open { id, chan })
                            .map_err(|e| e.to_string())?;

                        out_entry
                            .get_mut()
                            .start_send(msg)
                            .map_err(|e| e.to_string())?;
                    }
                    (Entry::Vacant(in_entry), Entry::Vacant(out_entry)) => {
                        // Dial and open a new general muxed stream.
                        let (_peer, mux) = transport
                            .clone()
                            .dial(maddr)
                            .map_err(|e| e.to_string())?
                            .await
                            .map_err(|e| e.to_string())?;
                        let mux = Arc::new(mux);

                        let (mut in_sender, in_receiver) = mpsc::channel(64);
                        in_entry.insert(in_sender.clone());

                        let inmux = mux.clone();
                        spawn(async move {
                            // This loop listens for inbound stream events and negociates for any of
                            // the supported protocols.
                            if let Err(e) = inbound_loop(in_receiver, inmux).await {
                                println!("inbound error: {:?}", e);
                            }
                        });
                        in_sender
                            .start_send(ChannelEvent::Open { id, chan })
                            .map_err(|e| e.to_string())?;

                        // This channel sends outbound requests to an open stream.
                        let (mut out_sender, out_receiver) = mpsc::channel(64);
                        out_entry.insert(out_sender.clone());

                        spawn(async move {
                            // This loop receives messages we want to send to the stream over a given
                            // protocol.
                            if let Err(e) = outbound_loop(out_receiver, mux).await {
                                println!("outbound error: {:?}", e);
                            }
                        });

                        // Send the first request message attached to the request event.
                        out_sender.start_send(msg).map_err(|e| e.to_string())?;
                    }
                    // both in and out should always be open for now
                    (_, _) => unreachable!(),
                };
            }
            // This event can be used to send a "oneshot" message to a given peer without expecting
            // any response.
            NetEvent::Message { peer, msg } => {
                if let Some(sender) = outbound.get_mut(&peer) {
                    sender.start_send(msg).map_err(|e| e.to_string())?;
                }
            }
            // Close any existing channel for a given request
            NetEvent::Cleanup { id, peer } => {
                if let Some(sender) = inbound.get_mut(&peer) {
                    sender
                        .start_send(ChannelEvent::Close(id))
                        .map_err(|e| e.to_string())?;
                }
            }
        }
    }
    Ok(())
}

enum Protocols {
    Graphsync,
    DataTransfer,
}

impl ProtocolName for Protocols {
    fn protocol_name(&self) -> &[u8] {
        match *self {
            Protocols::Graphsync => b"/ipfs/graphsync/1.0.0",
            Protocols::DataTransfer => b"/fil/datatransfer/1.2.0",
        }
    }
}

// The inbound loop listens for any muxer inbound event. If an event is received it will try
// negociating the protocol if supported and send it to the main inbound channel.
async fn inbound_loop(
    mut channels: mpsc::Receiver<ChannelEvent>,
    mux: Arc<StreamMuxerBox>,
) -> Result<(), String> {
    let mut open_channels: HashMap<RequestId, mpsc::Sender<NetMsg>> = HashMap::new();
    let protos: SmallVec<[Vec<u8>; 2]> = vec![Protocols::Graphsync, Protocols::DataTransfer]
        .into_iter()
        .map(|p| p.protocol_name().to_vec())
        .collect();
    let mut mux_futures = FuturesUnordered::new();
    mux_futures.push(event_from_ref_and_wrap(mux.clone()));
    loop {
        select! {
            channel_evt = channels.select_next_some() => {
                match channel_evt {
                    ChannelEvent::Open { id, chan } => {
                        open_channels.insert(id, chan);
                    }
                    ChannelEvent::Close(id) => {
                        open_channels.remove(&id);
                    }
                }
            },
            event = mux_futures.select_next_some() => {
                mux_futures.push(event_from_ref_and_wrap(mux.clone()));
                if let Some(inbound) = event.map_err(|e| e.to_string())?.into_inbound_substream() {
                    match listener_select_proto(inbound, protos.clone().into_iter()).await {
                        Ok((proto, mut io)) => match proto {
                            // handle graphsync protocol messages
                            gs_proto if gs_proto == Protocols::Graphsync.protocol_name() => {
                                let mut codec = GraphsyncCodec::<DefaultParams>::default();
                                // the response stream usually contains multiple messages
                                while let Ok(msg) = codec.read_message(&GraphsyncProtocol, &mut io).await {
                                    let req_ids: Vec<RequestId> =
                                        msg.responses.iter().map(|(_, r)| r.id).collect();
                                    for id in req_ids {
                                        if let Some(sender) = open_channels.get_mut(&id) {
                                            // there might be a way to avoid cloning by sending
                                            // blocks separately.
                                            sender
                                                .try_send(NetMsg::Graphsync(msg.clone()))
                                                .map_err(|e| e.to_string())?;
                                        }
                                    }
                                }
                                io.close().await.map_err(|e| e.to_string())?;
                            }
                            dt_proto if dt_proto == Protocols::DataTransfer.protocol_name() => {
                                let mut buf = Vec::new();
                                io.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
                                io.close().await.map_err(|e| e.to_string())?;
                                let msg = TransferMessage::unmarshal_cbor(&buf)
                                    .map_err(|_| "Failed to decode CBOR message".to_string())?;
                                // make sure the message isn't empty
                                if let (None, None) = (&msg.request, &msg.response) {
                                    continue;
                                }
                                // requests shouldn't be sent individually unless they're push
                                if msg.is_rq {
                                    unimplemented!("TODO");
                                } else {
                                    let id = msg.response.as_ref().expect("to be a response").transfer_id;
                                    if let Some(sender) = open_channels.get_mut(&(id as i32)) {
                                        sender
                                            .start_send(NetMsg::Transfer(msg))
                                            .map_err(|e| e.to_string())?;
                                    }
                                }
                            }
                            _ => unreachable!(),
                        },
                        Err(_) => continue,
                    }
                }
            },
        };
    }
}

async fn outbound_loop(
    mut messages: mpsc::Receiver<NetMsg>,
    mux: Arc<StreamMuxerBox>,
) -> Result<(), String> {
    while let Some(msg) = messages.next().await {
        let outbound = outbound_from_ref_and_wrap(mux.clone())
            .await
            .map_err(|e| e.to_string())?;

        let (_proto, mut io) = dialer_select_proto(
            outbound,
            vec![msg.protocol_name()].into_iter(),
            multistream_select::Version::V1,
        )
        .await
        .map_err(|e| e.to_string())?;

        match msg {
            NetMsg::Graphsync(msg) => {
                let mut codec = GraphsyncCodec::<DefaultParams>::default();
                codec
                    .write_message(&GraphsyncProtocol, &mut io, msg)
                    .await
                    .map_err(|e| e.to_string())?;
                io.close().await.map_err(|e| e.to_string())?;
            }
            NetMsg::Transfer(_msg) => (),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::DataTransferBehaviour;
    use super::*;
    use async_std::task;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use blockstore::types::DBStore;
    use dag_service::{add, cat};
    use futures::prelude::*;
    use graphsync::traversal::{RecursionLimit, Selector};
    use graphsync::{Config, Graphsync};
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
    use rand::prelude::*;
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
        swarm: Swarm<DataTransferBehaviour<MemoryBlockStore>>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let store = Arc::new(MemoryBlockStore::default());
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
                store,
            }
        }

        fn swarm(&mut self) -> &mut Swarm<DataTransferBehaviour<MemoryBlockStore>> {
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
        use futures::join;

        let peer1 = Peer::new();

        let store = peer1.store.clone();
        let maddr1 = peer1.addr.clone();
        let peer1 = peer1.spawn("peer1");

        const FILE_SIZE: usize = 1024 * 1024;
        let mut data1 = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data1);

        let mut data2 = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data2);

        let mut data3 = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data3);

        let mut data4 = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data4);

        let root1 = add(store.clone(), &data1).unwrap().unwrap();
        let root2 = add(store.clone(), &data2).unwrap().unwrap();
        let root3 = add(store.clone(), &data3).unwrap().unwrap();
        let root4 = add(store.clone(), &data4).unwrap().unwrap();

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

        let stream = client
            .pull(
                peer1,
                maddr1.clone(),
                root1,
                selector.clone(),
                Default::default(),
            )
            .await
            .unwrap();

        let mut reader = stream.reader();
        let mut output: Vec<u8> = Vec::new();
        assert_eq!(reader.read_to_end(&mut output).await.unwrap(), FILE_SIZE);
        assert_eq!(output, data1);

        let stream = client
            .pull(
                peer1,
                maddr1.clone(),
                root2,
                selector.clone(),
                Default::default(),
            )
            .await
            .unwrap();
        let mut reader = stream.reader();
        let mut output: Vec<u8> = Vec::new();
        assert_eq!(reader.read_to_end(&mut output).await.unwrap(), FILE_SIZE);
        assert_eq!(output, data2);

        // now let's try in parallel
        join!(
            async {
                let stream = client
                    .pull(
                        peer1,
                        maddr1.clone(),
                        root3,
                        selector.clone(),
                        Default::default(),
                    )
                    .await
                    .unwrap();
                let mut reader = stream.reader();
                let mut output: Vec<u8> = Vec::new();
                assert_eq!(reader.read_to_end(&mut output).await.unwrap(), FILE_SIZE);
                assert_eq!(output, data3);
            },
            async {
                let stream = client
                    .pull(
                        peer1,
                        maddr1.clone(),
                        root4,
                        selector.clone(),
                        Default::default(),
                    )
                    .await
                    .unwrap();
                let mut reader = stream.reader();
                let mut output: Vec<u8> = Vec::new();
                assert_eq!(reader.read_to_end(&mut output).await.unwrap(), FILE_SIZE);
                assert_eq!(output, data4);
            }
        );
    }
}
