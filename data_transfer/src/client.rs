use super::fsm::{Channel, ChannelEvent as FsmEvent};
use super::mimesniff::{detect_content_type, ContentType};
use super::network::{
    DealProposal, DealResponse, DealStatus, MessageType, PullParams, TransferMessage,
    TransferRequest, TransferResponse, EXTENSION_KEY,
};
use blockstore::types::BlockStore;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use futures::{
    channel::{mpsc, oneshot},
    executor::ThreadPoolBuilder,
    join,
    prelude::*,
    select,
    stream::{IntoAsyncRead, TryStreamExt},
};
use graphsync::network::MessageCodec;
use graphsync::res_mgr::ResponseBuilder;
use graphsync::traversal::{resolve_unixfs, AsyncLoader, Progress, Selector};
use graphsync::{
    Extensions, GraphsyncCodec, GraphsyncMessage, GraphsyncProtocol, GraphsyncRequest, Prefix,
    RequestId,
};
use libipld::codec::Decode;
use libipld::store::{DefaultParams, StoreParams};
use libipld::{Block, Cid, Ipld};
use libp2p::{
    core::muxing::{event_from_ref_and_wrap, outbound_from_ref_and_wrap, StreamMuxerBox},
    core::transport::{Boxed, ListenerEvent, Transport},
    core::{Executor, ProtocolName},
    multiaddr, Multiaddr, PeerId,
};
use multistream_select::{dialer_select_proto, listener_select_proto};
use num_bigint::ToBigInt;
use smallvec::SmallVec;
use std::collections::hash_map::{Entry, HashMap};
use std::io::{Error, ErrorKind};
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
    pub channel: mpsc::Receiver<Result<Vec<u8>, Error>>,
}

impl PullStream {
    pub fn reader(self) -> IntoAsyncRead<mpsc::Receiver<Result<Vec<u8>, Error>>> {
        self.channel.into_async_read()
    }
}

pub fn default_executor() -> Option<Box<dyn Executor>> {
    match ThreadPoolBuilder::new()
        .name_prefix("client-executor")
        .create()
    {
        Ok(tp) => Some(Box::new(move |f| tp.spawn_ok(f))),
        Err(err) => {
            log::warn!("Failed to create executor thread pool: {:?}", err);
            None
        }
    }
}

type AddrSendr = oneshot::Sender<SmallVec<[Multiaddr; 4]>>;
type AddrQuerySendr = mpsc::Sender<AddrSendr>;
type AddrQueryRecvr = mpsc::Receiver<AddrSendr>;

#[derive(Default)]
pub struct ClientOptions {
    executor: Option<Box<dyn Executor>>,
    listen_addr: Option<Multiaddr>,
}

impl ClientOptions {
    pub fn as_listener(mut self, addr: Multiaddr) -> Self {
        self.listen_addr = Some(addr);
        self
    }
    pub fn with_executor(mut self, executor: Box<dyn Executor>) -> Self {
        self.executor = Some(executor);
        self
    }
}

#[derive(Clone)]
pub struct Client<S: BlockStore> {
    pub store: Arc<S>,
    pub peer_id: PeerId,
    id_counter: Arc<AtomicI32>,
    outbound_events: mpsc::Sender<NetEvent>,
    addresses_query: Option<AddrQuerySendr>,
    executor: Arc<Box<dyn Executor>>,
}

impl<S: 'static + BlockStore> Client<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(
        peer_id: PeerId,
        store: Arc<S>,
        tp: Boxed<(PeerId, StreamMuxerBox)>,
        options: ClientOptions,
    ) -> Self {
        let (s, r) = mpsc::channel(128);
        let transport = tp.clone();
        let executor = Arc::new(options.executor.or_else(default_executor).unwrap());
        executor.exec(
            async move {
                if let Err(e) = conn_loop(r, transport).await {
                    println!("connection loop: {:?}", e);
                }
            }
            .boxed(),
        );
        // If the host isn't listening we cannot query the current addresses.
        let mut addresses_query = None;
        if let Some(addr) = options.listen_addr {
            let (query_sender, query_receiver) = mpsc::channel(4);
            addresses_query.replace(query_sender);
            let store = store.clone();
            // we don't use the executor here as it's mainly for wasm which doesn't need this loop.
            spawn(async move {
                match tp.listen_on(addr) {
                    Ok(listener) => {
                        if let Err(e) = accept_loop::<Boxed<(PeerId, StreamMuxerBox)>, S>(
                            listener,
                            store,
                            query_receiver,
                        )
                        .await
                        {
                            println!("listener failed: {:?}", e);
                        }
                    }
                    Err(e) => {
                        println!("failed to start listener: {:?}", e);
                    }
                }
            });
        }
        Self {
            store,
            peer_id,
            id_counter: Arc::new(AtomicI32::new(1)),
            outbound_events: s,
            addresses_query,
            executor,
        }
    }
    /// returns the listener addresses. If they have not been anounced yet waits for the first one.
    /// addresses are formatted as p2p including the peer id.
    pub async fn addresses(&mut self) -> Option<SmallVec<[Multiaddr; 4]>> {
        if let Some(query) = self.addresses_query.as_mut() {
            let (s, r) = oneshot::channel();
            query.try_send(s).ok()?;
            let a = r.await.ok()?;
            return Some(
                a.iter()
                    .map(|addr| {
                        addr.clone()
                            .with(multiaddr::Protocol::P2p(self.peer_id.into()))
                    })
                    .collect(),
            );
        }
        None
    }
    pub async fn pull(
        &self,
        peer: PeerId,
        maddr: Multiaddr,
        root: Cid,
        sel: Selector,
        params: PullParams,
    ) -> Result<PullStream, Error> {
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
        let buf = message.marshal_cbor().map_err(io_err)?;
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
        let (inbound_send, inbound_receive) = mpsc::channel(64);

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
            .map_err(io_err)?;

        // This is the final channel that will consume the transfer data.
        let (mut s, r) = mpsc::channel(64);
        // This channel waits for the first bytes to come in.
        let (os, or) = oneshot::channel();

        let store = self.store.clone();
        self.executor.exec(async move {
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

            let mut inbound = inbound_receive.fuse();
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
                    // If the channel is closed before we recived all the messages something went
                    // wrong i.e. the provider closed the connection.
                    res = inbound.next().fuse() => match res {
                        Some(ev) => {
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
                        }
                        None => {
                            s.start_send(Err(Error::from(ErrorKind::UnexpectedEof))).unwrap();
                            break;
                        }
                    },
                };
            }
        }.boxed());
        // Wait to receive the content type before returning the stream
        let content_type = or.await.map_err(io_err)?;
        Ok(PullStream {
            content_type,
            channel: r,
        })
    }
}

fn io_err<E: Into<Box<dyn std::error::Error + Send + Sync>>>(e: E) -> Error {
    Error::new(ErrorKind::Other, e)
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

struct Connection {
    stream: Arc<StreamMuxerBox>,
    requests: SmallVec<[RequestId; 4]>,
}

impl Connection {
    fn new(stream: Arc<StreamMuxerBox>, req: RequestId) -> Self {
        Self {
            stream,
            requests: SmallVec::from_vec(vec![req]),
        }
    }
}

// The accept loop processes inbound request sent to the listener.
async fn accept_loop<TTrans, S>(
    listener: TTrans::Listener,
    store: Arc<S>,
    mut addr: AddrQueryRecvr,
) -> Result<(), Error>
where
    TTrans: Transport<Output = (PeerId, StreamMuxerBox)>,
    TTrans::Error: Send + Sync + 'static,
    TTrans::ListenerUpgrade:
        Future<Output = Result<(PeerId, StreamMuxerBox), Error>> + Send + 'static,
    S: 'static + BlockStore,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    let mut listen = Box::pin(listener);
    let mut addresses: SmallVec<[Multiaddr; 4]> = SmallVec::new();

    let mut pending_addr_query: Option<AddrSendr> = None;
    loop {
        select! {
            result = listen.next().fuse() => match result {
                Some(Ok(event)) => {
                    match event {
                        ListenerEvent::Upgrade {
                            upgrade,
                            local_addr: _,
                            remote_addr: _,
                        } => {
                            let store = store.clone();
                            spawn(async move {
                                match upgrade.await {
                                    Ok((_peer, muxer)) => {
                                        if let Err(e) = listener_upgrade_handler::<S>(muxer, store).await {
                                            println!("upgrade error: {:?}", e);
                                        }
                                    }
                                    Err(e) => {
                                        println!("upgrade failed: {:?}", e);
                                    }
                                }
                            });
                        }
                        ListenerEvent::NewAddress(addr) => {
                            if !addresses.contains(&addr) {
                                addresses.push(addr);
                            }
                            if let Some(query) = pending_addr_query.take() {
                                drop(query.send(addresses.clone()));
                            };
                        }
                        ListenerEvent::AddressExpired(addr) => {
                            addresses.retain(|x| x != &addr);
                        }
                        _ => (),
                    };
                }
                _ => {}
            },
            query = addr.select_next_some() => {
                if addresses.is_empty() {
                    pending_addr_query.replace(query);
                } else {
                    drop(query.send(addresses.clone()));
                }
            },
        }
    }
}

async fn listener_upgrade_handler<S>(muxer: StreamMuxerBox, store: Arc<S>) -> Result<(), Error>
where
    S: 'static + BlockStore,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    let mux = Arc::new(muxer);

    let (in_sender, mut in_receiver) = mpsc::channel(16);
    let inmux = mux.clone();
    spawn(async move {
        if let Err(e) = inbound_loop(in_sender.clone(), inmux).await {
            println!("inbound error: {:?}", e);
        }
    });

    while let Some(Ok(msg)) = in_receiver.next().await {
        match msg {
            NetMsg::Graphsync(msg) => {
                if !msg.requests.is_empty() {
                    for req in msg.requests.values() {
                        let outmux = mux.clone();
                        let request = req.clone();
                        let store = store.clone();
                        spawn(async move {
                            if let Err(e) = graphsync_provider::<S>(store, request, outmux).await {
                                println!("graphsync provider error: {:?}", e);
                            }
                        });
                    }
                }
            }
            NetMsg::Transfer(msg) => {
                unimplemented!("TODO");
            }
        }
    }
    Ok(())
}

async fn graphsync_provider<S>(
    store: Arc<S>,
    request: GraphsyncRequest,
    mux: Arc<StreamMuxerBox>,
) -> Result<(), Error>
where
    S: 'static + BlockStore,
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    let outbound = outbound_from_ref_and_wrap(mux.clone()).await?;

    let (_proto, mut io) = dialer_select_proto(
        outbound,
        vec![Protocols::Graphsync.protocol_name()].into_iter(),
        multistream_select::Version::V1,
    )
    .await?;

    let mut codec = GraphsyncCodec::<DefaultParams>::default();
    let mut builder = ResponseBuilder::new(&request, store);
    while let Some(msg) = builder.next() {
        codec
            .write_message(&GraphsyncProtocol, &mut io, msg)
            .await?;
    }
    io.close().await?;

    let id = request.id as u64;
    let voucher = DealResponse::Pull {
        id,
        status: DealStatus::Completed,
        message: "Thanks for doing business with us".to_string(),
        payment_owed: (0_isize).to_bigint().unwrap(),
    };
    let tmsg = TransferMessage {
        is_rq: false,
        request: None,
        response: Some(TransferResponse {
            mtype: MessageType::Complete,
            accepted: true,
            paused: false,
            transfer_id: id,
            voucher_type: voucher.voucher_type(),
            voucher: Some(voucher),
        }),
    };

    let outbound = outbound_from_ref_and_wrap(mux).await?;

    let (_proto, mut io) = dialer_select_proto(
        outbound,
        vec![Protocols::DataTransfer.protocol_name()].into_iter(),
        multistream_select::Version::V1,
    )
    .await?;

    let buf = tmsg.marshal_cbor().map_err(io_err)?;
    io.write_all(&buf).await?;
    io.close().await?;

    Ok(())
}

// The connection loop is responsible for opening new streams with peer or sending to open
// connections. The request contains a sender to collect inbound messages.
async fn conn_loop(
    mut events: mpsc::Receiver<NetEvent>,
    transport: Boxed<(PeerId, StreamMuxerBox)>,
) -> Result<(), Error> {
    // here we maintain a list of open connections with peers. If a peer isn't included in this
    // list we dial and open a new stream. Channel sends outbound messages.
    let mut conns: HashMap<PeerId, Connection> = HashMap::new();

    // channels for each request are maintained here
    let mut inbound: HashMap<RequestId, mpsc::Sender<NetMsg>> = HashMap::new();

    // global inbound channel, all inbound messages are sent through
    let (in_sender, mut in_receiver) = mpsc::channel(64);

    loop {
        select! {
            // Receive all the messages for a single peer inbound stream and forward them to the
            // corresponding channels.
            result = in_receiver.select_next_some() => {
                match result {
                    Ok(msg) => match msg {
                    NetMsg::Graphsync(msg) => {
                        let req_ids: Vec<RequestId> =
                            msg.responses.iter().map(|(_, r)| r.id).collect();
                        for id in req_ids {
                            if let Some(sender) = inbound.get_mut(&id) {
                            // there might be a way to avoid cloning by sending
                            // blocks separately.
                            sender
                                .try_send(NetMsg::Graphsync(msg.clone()))
                                .map_err(io_err)?;
                            }
                        }
                    }
                    NetMsg::Transfer(msg) => {
                        // make sure the message isn't empty
                        if let (None, None) = (&msg.request, &msg.response) {
                            continue;
                        }
                        // requests shouldn't be sent individually unless they're push
                        if msg.is_rq {
                            unimplemented!("TODO");
                        } else {
                            let id = msg.response.as_ref().expect("to be a response").transfer_id;
                            if let Some(sender) = inbound.get_mut(&(id as i32)) {
                                sender
                                    .try_send(NetMsg::Transfer(msg))
                                    .map_err(io_err)?;
                            }
                        }
                    }
                    },
                    Err(e) => {
                        let e: InboundError = e;
                        // The peer disconnected so we remove the muxer and shut down any
                        // open channels.
                        match e.err.kind() {
                            ErrorKind::UnexpectedEof
                            | ErrorKind::BrokenPipe
                            | ErrorKind::ConnectionReset => {
                                if let Some(peer) = e.peer {
                                    if let Some(conn) = conns.remove(&peer) {
                                        for id in conn.requests {
                                            if let Some(mut sender) = inbound.remove(&id) {
                                                sender.close_channel();
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                println!("muxer error {:?}", e);
                                // TODO: if something goes wrong for a specific channel we
                                // should notify it so it can close up.
                            }
                        }
                    }
                };
            },
            ev = events.select_next_some() => {
            match ev {
                NetEvent::NewRequest {
                    id,
                    peer,
                    maddr,
                    msg,
                    chan,
                } => {
                    match conns.entry(peer) {
                        Entry::Occupied(mut entry) => {
                            // We already have an open connection from a previous transfer
                            inbound.insert(id, chan);

                            // keep track of ongoing requests with the connection
                            let conn = entry.get_mut();
                            conn.requests.push(id);
                            let mux = conn.stream.clone();
                            spawn(async move {
                                if let Err(e) = outbound_write(msg, mux).await {
                                    log::info!("failed to write outbound: {:?}", e);
                                }
                            });
                        }
                        Entry::Vacant(entry) => {
                            // Dial and open a new general muxed stream.
                            let (_peer, mux) = transport
                                .clone()
                                .dial(maddr)
                                .map_err(io_err)?
                                .await
                                .map_err(io_err)?;
                            let mux = Arc::new(mux);

                            inbound.insert(id, chan);

                            let inmux = mux.clone();
                            let mut inbound_send = in_sender.clone();
                            spawn(async move {
                                // This loop listens for inbound stream events and negociates for any of
                                // the supported protocols.
                                if let Err(e) = inbound_loop(inbound_send.clone(), inmux).await {
                                    println!("inbound error: {:?}", e);
                                    inbound_send
                                        .try_send(Err(InboundError { err: e, peer: Some(peer) }))
                                        .unwrap();
                                }
                            });

                            let outmux = mux.clone();
                            spawn(async move {
                                if let Err(e) = outbound_write(msg, outmux).await {
                                    log::info!("failed to write outbound: {:?}", e);
                                }
                            });

                            entry.insert(Connection::new(mux, id));
                        }
                    };
                }
                // This event can be used to send a "oneshot" message to a given peer without expecting
                // any response.
                NetEvent::Message { peer, msg } => {
                    if let Some(conn) = conns.get(&peer) {
                        let outmux = conn.stream.clone();
                        spawn(async move {
                            if let Err(e) = outbound_write(msg, outmux).await {
                                log::info!("failed to write outbound: {:?}", e);
                            }
                        });
                    }
                }
                // Close any existing channel for a given request
                NetEvent::Cleanup { id, peer } => {
                    if let Some(conn) = conns.get_mut(&peer) {
                        conn.requests.sort();
                        if let Ok(idx) = conn.requests.binary_search(&id) {
                            conn.requests.remove(idx);
                        }
                    }
                    inbound.remove(&id);
                }
            }
        },
        }
    }
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

#[derive(Debug)]
struct InboundError {
    peer: Option<PeerId>,
    err: Error,
}

// The inbound loop listens for any muxer inbound event. If an event is received it will try
// negociating the protocol if supported and send it to the main inbound channel.
// If an error is polled from the inbound, the loop will terminate and return the error.
async fn inbound_loop(
    messages: mpsc::Sender<Result<NetMsg, InboundError>>,
    mux: Arc<StreamMuxerBox>,
) -> Result<(), Error> {
    let protos: SmallVec<[Vec<u8>; 2]> = vec![Protocols::Graphsync, Protocols::DataTransfer]
        .into_iter()
        .map(|p| p.protocol_name().to_vec())
        .collect();
    while let Some(inbound) = event_from_ref_and_wrap(mux.clone())
        .await?
        .into_inbound_substream()
    {
        let ps = protos.clone();
        let sender = messages.clone();
        spawn(async move {
            match listener_select_proto(inbound, ps.into_iter()).await {
                Ok((proto, mut io)) => {
                    let mut send_err = sender.clone();
                    if let Err(e) = read_proto(&mut io, &proto, sender).await {
                        send_err
                            .try_send(Err(InboundError { err: e, peer: None }))
                            .unwrap();
                    }
                }
                Err(_) => {
                    // We received inbound messages for unsuported protocols: Ignore.
                }
            }
        });
    }
    Ok(())
}

async fn read_proto<R>(
    io: &mut R,
    proto: &[u8],
    mut sender: mpsc::Sender<Result<NetMsg, InboundError>>,
) -> Result<(), Error>
where
    R: AsyncRead + AsyncWrite + Unpin + Send,
{
    match proto {
        // handle graphsync protocol messages
        gs_proto if gs_proto == Protocols::Graphsync.protocol_name() => {
            let mut codec = GraphsyncCodec::<DefaultParams>::default();
            // the response stream usually contains multiple messages
            while let Ok(msg) = codec.read_message(&GraphsyncProtocol, io).await {
                // there might be a way to avoid cloning by sending
                // blocks separately.
                sender
                    .try_send(Ok(NetMsg::Graphsync(msg)))
                    .map_err(io_err)?;
            }
            io.close().await?;
        }
        dt_proto if dt_proto == Protocols::DataTransfer.protocol_name() => {
            let mut buf = Vec::new();
            io.read_to_end(&mut buf).await?;
            io.close().await?;
            let msg = TransferMessage::unmarshal_cbor(&buf).map_err(io_err)?;
            // make sure the message isn't empty
            sender.try_send(Ok(NetMsg::Transfer(msg))).map_err(io_err)?;
        }
        _ => unreachable!(),
    };
    Ok(())
}

async fn outbound_write(msg: NetMsg, mux: Arc<StreamMuxerBox>) -> Result<(), Error> {
    let outbound = outbound_from_ref_and_wrap(mux).await?;

    let (_proto, mut io) = dialer_select_proto(
        outbound,
        vec![msg.protocol_name()].into_iter(),
        multistream_select::Version::V1,
    )
    .await?;

    match msg {
        NetMsg::Graphsync(msg) => {
            let mut codec = GraphsyncCodec::<DefaultParams>::default();
            codec
                .write_message(&GraphsyncProtocol, &mut io, msg)
                .await?;
            io.close().await?;
        }
        NetMsg::Transfer(_msg) => (),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{DataTransferBehaviour, DataTransferEvent};
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
    use std::thread;
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

        fn spawn(mut self, name: &'static str) -> PeerId {
            let peer_id = self.peer_id;
            task::spawn(async move {
                loop {
                    let event = self.swarm.next().await;
                    println!("event {:?}", event);
                    if name == "drop_completed" {
                        if let Some(SwarmEvent::Behaviour(DataTransferEvent::Completed(_, _))) =
                            event
                        {
                            break;
                        }
                    }
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

        let client = Client::new(peer2, store2, trans, Default::default());

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

    // This test disconnects the peer right after completing leaving no time for the client
    // to read the remaining of the inbound messages.
    #[async_std::test]
    async fn test_client_disconnect() {
        use futures::join;

        let peer1 = Peer::new();

        let store = peer1.store.clone();
        let maddr1 = peer1.addr.clone();
        let peer1 = peer1.spawn("drop_completed");

        const FILE_SIZE: usize = 1024 * 1024;
        let mut data1 = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data1);

        let root1 = add(store.clone(), &data1).unwrap().unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let (peer2, trans) = mk_transport();
        let store2 = Arc::new(MemoryBlockStore::default());

        let client = Client::new(peer2, store2, trans, Default::default());

        let maddr1 = maddr1.with(multiaddr::Protocol::P2p(peer1.into()));

        match client
            .pull(
                peer1,
                maddr1.clone(),
                root1,
                selector.clone(),
                Default::default(),
            )
            .await
        {
            Ok(stream) => {
                let mut reader = stream.reader();
                let mut output: Vec<u8> = Vec::new();
                if let Err(e) = reader.read_to_end(&mut output).await {
                    assert_eq!(e.kind(), ErrorKind::UnexpectedEof);
                } else {
                    panic!("transfer should be interupted");
                }
            }
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::Other);
            }
        };
    }

    #[async_std::test]
    async fn test_provider() {
        let store1 = Arc::new(MemoryBlockStore::default());

        const FILE_SIZE: usize = 1024 * 1024;
        let mut data1 = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data1);

        let root1 = add(store1.clone(), &data1).unwrap().unwrap();

        let (peer1, trans) = mk_transport();
        let mut provider = Client::new(
            peer1.clone(),
            store1,
            trans,
            ClientOptions {
                executor: None,
                listen_addr: Some("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
            },
        );

        let mut addresses = provider.addresses().await.unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let (peer2, trans) = mk_transport();
        let store2 = Arc::new(MemoryBlockStore::default());

        let client = Client::new(peer2, store2, trans, Default::default());

        let stream = client
            .pull(
                peer1,
                addresses.pop().unwrap(),
                root1,
                selector,
                Default::default(),
            )
            .await
            .unwrap();

        let mut reader = stream.reader();
        let mut output: Vec<u8> = Vec::new();
        assert_eq!(reader.read_to_end(&mut output).await.unwrap(), FILE_SIZE);
        assert_eq!(output, data1);
    }
}
