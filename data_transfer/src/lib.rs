mod fsm;
pub mod mimesniff;
mod network;

pub use network::{
    ChannelId, DataTransferNetwork, DtNetEvent, PullParams, PushParams, TransferMessage,
    EXTENSION_KEY,
};

use blockstore::types::BlockStore;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use fsm::{Channel, ChannelEvent};
use graphsync::traversal::Selector;
use graphsync::{
    client::Client, Extensions, Graphsync, GraphsyncEvent, IncomingRequestHook, RequestId,
};
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::{Multiaddr, NetworkBehaviour, PeerId};
use network::{
    DealProposal, DealResponse, DealStatus, MessageType, TransferRequest, TransferResponse,
    EMPTY_QUEUE_SHRINK_THRESHOLD,
};
use num_bigint::ToBigInt;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    task::{Context, Poll},
};

pub struct DtClient<S: BlockStore> {
    gs_client: Client<S>,
    next_request_id: u64,
}

impl<S: 'static + BlockStore> DtClient<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(gs_client: Client<S>) -> Self {
        let next_request_id = instant::now() as u64;
        Self {
            next_request_id,
            gs_client,
        }
    }
}

pub struct DataTransfer {
    peer_id: PeerId,
    next_request_id: u64,
    pending_events: VecDeque<DataTransferEvent>,
    channels: HashMap<ChannelId, Channel>,
    pub channel_ids: HashMap<RequestId, ChannelId>,
}

impl DataTransfer {
    pub fn new(peer_id: PeerId) -> Self {
        let now_unix = instant::now() as u64;
        Self {
            peer_id,
            next_request_id: now_unix,
            pending_events: VecDeque::default(),
            channel_ids: HashMap::new(),
            channels: HashMap::new(),
        }
    }

    pub fn open_pull_channel(
        &mut self,
        peer_id: PeerId,
        root: Cid,
        selector: Selector,
        params: PullParams,
    ) -> (ChannelId, TransferMessage) {
        let cid = CidCbor::from(root);
        let request_id = self.next_request_id();
        let voucher = DealProposal::Pull {
            id: request_id,
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
                selector: selector.clone(),
                voucher_type: voucher.voucher_type(),
                voucher: Some(voucher),
                transfer_id: request_id,
                restart_channel: Default::default(),
            }),
            response: None,
        };

        let ch_id = self.channel_id(request_id, peer_id);
        self.channels.insert(
            ch_id.clone(),
            Channel::New {
                id: ch_id.clone(),
                deal_id: request_id,
            },
        );
        (ch_id, message)
    }

    pub fn open_push_channel(
        &mut self,
        peer_id: PeerId,
        root: Cid,
        selector: Selector,
        params: PushParams,
    ) -> (ChannelId, TransferMessage) {
        let cid = CidCbor::from(root);
        let request_id = self.next_request_id();
        let voucher = DealProposal::Push {
            id: request_id,
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
                pull: false,
                selector: selector.clone(),
                voucher_type: voucher.voucher_type(),
                voucher: Some(voucher),
                transfer_id: request_id,
                restart_channel: Default::default(),
            }),
            response: None,
        };
        let ch_id = self.channel_id(request_id, peer_id);
        self.channels.insert(
            ch_id.clone(),
            Channel::New {
                id: ch_id.clone(),
                deal_id: request_id,
            },
        );
        (ch_id, message)
    }

    /// Constructs a channel id
    fn channel_id(&self, id: u64, responder: PeerId) -> ChannelId {
        ChannelId {
            initiator: self.peer_id.to_base58(),
            responder: responder.to_base58(),
            id,
        }
    }

    /// Returns the next request ID.
    fn next_request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        request_id
    }

    fn get_channel_by_req_id(&mut self, req_id: RequestId) -> Option<(ChannelId, Channel)> {
        let ch_id = self.channel_ids.get(&req_id)?;
        let ch = self.channels.remove(ch_id)?;
        Some((ch_id.clone(), ch))
    }

    fn process_request(&mut self, peer: PeerId, request: TransferRequest) -> ChannelId {
        let ch_id = self.channel_id(request.transfer_id, peer);
        match request.voucher {
            Some(DealProposal::Pull { id, .. }) => {
                self.channels.insert(
                    ch_id.clone(),
                    Channel::New {
                        id: ch_id.clone(),
                        deal_id: id,
                    },
                );
            }
            Some(DealProposal::Push { id, .. }) => {
                // start the state as accepted as the receiver accepts to receive the content
                self.channels.insert(
                    ch_id.clone(),
                    Channel::Accepted {
                        id: ch_id.clone(),
                        deal_id: id,
                    },
                );
                if let Some(cid) = request.root.to_cid() {
                    let voucher = DealResponse::Push {
                        id,
                        status: DealStatus::Accepted,
                        message: "".to_string(),
                        secret: "".to_string(),
                    };
                    let tmsg = TransferMessage {
                        is_rq: false,
                        request: None,
                        response: Some(TransferResponse {
                            mtype: MessageType::New,
                            accepted: true,
                            paused: false,
                            transfer_id: request.transfer_id,
                            voucher_type: voucher.voucher_type(),
                            voucher: Some(voucher),
                        }),
                    };
                    let mut extensions = HashMap::new();
                    extensions.insert(EXTENSION_KEY.to_string(), tmsg.marshal_cbor().unwrap());
                    // in the case pf a push request, the responder initiates the graphsync request
                    self.pending_events
                        .push_back(DataTransferEvent::GsOutbound {
                            ch_id: ch_id.clone(),
                            peer_id: peer,
                            root: cid,
                            selector: request.selector,
                            extensions,
                        });
                }
            }
            _ => {}
        }
        ch_id
    }

    fn process_response(&mut self, peer: PeerId, response: TransferResponse) -> ChannelId {
        let ch_id = self.channel_id(response.transfer_id, peer);
        if response.accepted {
            match response.voucher {
                Some(DealResponse::Pull { status, .. }) => {
                    match status {
                        // if it's the response for a pull request, it was attached to the first graphsync
                        // message.
                        DealStatus::Accepted => {
                            let ch = self
                                .channels
                                .remove(&ch_id)
                                .expect("Expected channel to be created before accepted");
                            let next_state = ch.transition(ChannelEvent::Accepted);
                            self.channels.insert(ch_id.clone(), next_state.clone());
                            self.pending_events.push_back(next_state.into());
                        }
                        // the completion response for a pull request is sent separately after the
                        // transfer is completed.
                        DealStatus::Completed => {
                            let ch = self
                                .channels
                                .remove(&ch_id)
                                .expect("Expected channel to be created before response");
                            let next_state = ch.transition(ChannelEvent::Completed);
                            self.channels.insert(ch_id.clone(), next_state.clone());
                            self.pending_events.push_back(next_state.into());
                        }
                        _ => {}
                    }
                }
                Some(DealResponse::Push { status, .. }) => {
                    match status {
                        // the response for a push request is sent as attachment to the graphsync
                        // request.
                        DealStatus::Accepted => {
                            let ch = self
                                .channels
                                .remove(&ch_id)
                                .expect("Expected channel to be created before accepted");
                            let next_state = ch.transition(ChannelEvent::Accepted);
                            self.channels.insert(ch_id.clone(), next_state.clone());
                            self.pending_events.push_back(next_state.into());
                        }
                        // completed response is processed on the push receiver side to notify we have
                        // sent all the blocks.
                        DealStatus::Completed => {
                            let ch = self
                                .channels
                                .remove(&ch_id)
                                .expect("Expected channel to be created before response");
                            let next_state = ch.transition(ChannelEvent::Completed);
                            self.channels.insert(ch_id.clone(), next_state.clone());
                            self.pending_events.push_back(next_state.into());
                        }
                        _ => {}
                    }
                }
                // response is invalid as it didn't contain a voucher
                _ => {}
            }
        } else {
            log::info!("response {:?}", response);
            unimplemented!("TODO");
        }
        ch_id
    }

    pub fn inject_graphsync_event(&mut self, event: GraphsyncEvent) {
        match event {
            GraphsyncEvent::RequestAccepted(peer, req) => {
                if let Ok(msg) = TransferMessage::try_from(&req.extensions) {
                    if msg.is_rq {
                        let ch_id = self.process_request(
                            peer,
                            msg.request
                                .expect("Expected graphsync request to contain request"),
                        );
                        self.channel_ids.insert(req.id, ch_id);
                    } else {
                        let ch_id = self.process_response(
                            peer,
                            msg.response
                                .expect("Expected graphsync request to contain response"),
                        );
                        self.channel_ids.insert(req.id, ch_id);
                    }
                }
            }
            GraphsyncEvent::ResponseCompleted(peer, req_ids) => {
                for id in req_ids {
                    // we are removing the channel here
                    let (ch_id, ch) = self.get_channel_by_req_id(id).expect(
                        "Expected channel to be created before graphsync response is completed",
                    );
                    match ch {
                        // Accepted state is for push requests
                        Channel::Accepted { deal_id, .. } => {
                            let voucher = DealResponse::Push {
                                id: deal_id,
                                status: DealStatus::Completed,
                                message: "".to_string(),
                                secret: "".to_string(),
                            };
                            let tmsg = TransferMessage {
                                is_rq: false,
                                request: None,
                                response: Some(TransferResponse {
                                    mtype: MessageType::Complete,
                                    accepted: true,
                                    paused: false,
                                    transfer_id: ch_id.id,
                                    voucher_type: voucher.voucher_type(),
                                    voucher: Some(voucher),
                                }),
                            };
                            self.pending_events
                                .push_back(DataTransferEvent::DtOutbound(peer, tmsg));
                            // after we sent the completed message our transfer is complete.
                            let next_state = Channel::Completed {
                                id: ch_id,
                                received: 0,
                            };
                            self.pending_events.push_back(next_state.into());
                        }
                        // as the responder we sent the message confirming we are done sending all
                        // the blocks.
                        Channel::New { deal_id, .. } => {
                            let voucher = DealResponse::Pull {
                                id: deal_id,
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
                                    transfer_id: ch_id.id,
                                    voucher_type: voucher.voucher_type(),
                                    voucher: Some(voucher),
                                }),
                            };
                            self.pending_events
                                .push_back(DataTransferEvent::DtOutbound(peer, tmsg));
                        }
                        _ => {}
                    }
                }
            }
            GraphsyncEvent::ResponseReceived(peer, responses) => {
                for res in responses.iter() {
                    if let Ok(res) = TransferMessage::try_from(&res.extensions) {
                        if let Some(response) = res.response {
                            self.process_response(peer, response);
                        }
                    };
                }
            }
            GraphsyncEvent::Progress {
                req_id,
                link,
                size,
                data,
            } => {
                let (ch_id, ch) = self
                    .get_channel_by_req_id(req_id)
                    .expect("Expected channel to be created before graphsync progress");
                let event = ChannelEvent::BlockReceived { size };
                let next_state = ch.transition(event);
                self.channels.insert(ch_id.clone(), next_state.clone());
                self.pending_events.push_back(DataTransferEvent::Block {
                    ch_id,
                    link,
                    size,
                    data,
                });
                self.pending_events.push_back(next_state.into());
            }
            GraphsyncEvent::Complete(req_id, result) => {
                let (ch_id, ch) = self
                    .get_channel_by_req_id(req_id)
                    .expect("Expected channel to be created before graphsync complete");
                let event = match result {
                    Ok(()) => ChannelEvent::AllBlocksReceived,
                    Err(e) => ChannelEvent::Failure {
                        reason: e.to_string(),
                    },
                };
                let next_state = ch.transition(event);
                self.channels.insert(ch_id, next_state.clone());
                self.pending_events.push_back(next_state.into());
            }
        }
    }

    pub fn inject_dt_event(&mut self, event: DtNetEvent) {
        match event {
            DtNetEvent::Request(peer_id, request) => {
                self.process_request(peer_id, request);
            }
            DtNetEvent::Response(peer_id, response) => {
                self.process_response(peer_id, response);
            }
        }
    }

    pub fn incoming_request_hook(&self) -> IncomingRequestHook {
        Arc::new(|_peer, gs_req| {
            let mut extensions = HashMap::new();
            if let Ok(rmsg) = TransferMessage::try_from(&gs_req.extensions) {
                if rmsg.is_rq {
                    let req = rmsg
                        .request
                        .expect("Expected incoming message to have a request");
                    if let DealProposal::Pull { id, .. } =
                        req.voucher.expect("Expected request to have a voucher")
                    {
                        let voucher = DealResponse::Pull {
                            id,
                            status: DealStatus::Accepted,
                            message: "".to_string(),
                            payment_owed: (0_isize).to_bigint().unwrap(),
                        };
                        let tmsg = TransferMessage {
                            is_rq: false,
                            request: None,
                            response: Some(TransferResponse {
                                mtype: MessageType::New,
                                accepted: true,
                                paused: false,
                                transfer_id: req.transfer_id,
                                voucher_type: voucher.voucher_type(),
                                voucher: Some(voucher),
                            }),
                        };
                        extensions.insert(EXTENSION_KEY.to_string(), tmsg.marshal_cbor().unwrap());
                    }
                }
                (true, extensions)
            } else {
                (false, extensions)
            }
        })
    }
}

#[derive(Debug)]
pub enum DataTransferEvent {
    DtOutbound(PeerId, TransferMessage),
    GsOutbound {
        ch_id: ChannelId,
        peer_id: PeerId,
        root: Cid,
        selector: Selector,
        extensions: Extensions,
    },
    Started(ChannelId),
    Accepted(ChannelId),
    Progress(ChannelId),
    Block {
        ch_id: ChannelId,
        link: Cid,
        size: usize,
        data: Ipld,
    },
    Completed(ChannelId, Result<(), String>),
}

#[derive(NetworkBehaviour)]
#[behaviour(
    out_event = "DataTransferEvent",
    poll_method = "poll",
    event_process = true
)]
pub struct DataTransferBehaviour<S: BlockStore>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    graphsync: Graphsync<S>,
    network: DataTransferNetwork,

    #[behaviour(ignore)]
    dt: DataTransfer,
}

impl<S: 'static + BlockStore> DataTransferBehaviour<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(peer_id: PeerId, mut graphsync: Graphsync<S>) -> Self {
        let dt = DataTransfer::new(peer_id);
        graphsync.register_incoming_request_hook(dt.incoming_request_hook());

        Self {
            graphsync,
            network: DataTransferNetwork::new(),
            dt,
        }
    }

    pub fn pull(
        &mut self,
        peer_id: PeerId,
        root: Cid,
        selector: Selector,
        params: PullParams,
    ) -> Result<ChannelId, String> {
        let (ch_id, message) = self
            .dt
            .open_pull_channel(peer_id, root, selector.clone(), params);
        let buf = message.marshal_cbor().map_err(|e| e.to_string())?;
        let mut extensions = HashMap::new();
        extensions.insert(EXTENSION_KEY.to_string(), buf);
        let rq_id = self.graphsync.request(peer_id, root, selector, extensions);
        self.dt.channel_ids.insert(rq_id, ch_id.clone());
        Ok(ch_id)
    }

    pub fn push(
        &mut self,
        peer_id: PeerId,
        root: Cid,
        selector: Selector,
        params: PushParams,
    ) -> Result<ChannelId, String> {
        let (ch_id, message) = self
            .dt
            .open_push_channel(peer_id, root, selector.clone(), params);
        self.network.send_message(&peer_id, message);
        Ok(ch_id)
    }

    fn poll(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self as NetworkBehaviour>::OutEvent,
            <Self as NetworkBehaviour>::ProtocolsHandler,
        >,
    > {
        if let Some(ev) = self.dt.pending_events.pop_front() {
            match ev {
                DataTransferEvent::DtOutbound(peer, msg) => {
                    self.network.send_message(&peer, msg);
                }
                DataTransferEvent::GsOutbound {
                    ch_id,
                    peer_id,
                    root,
                    selector,
                    extensions,
                } => {
                    let rq_id = self.graphsync.request(peer_id, root, selector, extensions);
                    self.dt.channel_ids.insert(rq_id, ch_id);
                }
                _ => {
                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(ev));
                }
            }
        } else if self.dt.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.dt.pending_events.shrink_to_fit();
        }
        Poll::Pending
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.graphsync.add_address(peer_id, addr.clone());
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<GraphsyncEvent>
    for DataTransferBehaviour<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: GraphsyncEvent) {
        self.dt.inject_graphsync_event(event);
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<DtNetEvent> for DataTransferBehaviour<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: DtNetEvent) {
        self.dt.inject_dt_event(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use dag_service::{add, cat};
    use futures::prelude::*;
    use graphsync::traversal::RecursionLimit;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::identity;
    use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::TcpConfig;
    use libp2p::yamux::YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
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
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .boxed();
        (peer_id, transport)
    }

    struct Peer {
        peer_id: PeerId,
        addr: Multiaddr,
        swarm: Swarm<DataTransferBehaviour<MemoryBlockStore>>,
        store: Arc<MemoryBlockStore>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let bs = Arc::new(MemoryBlockStore::default());
            let gs = Graphsync::new(Default::default(), bs.clone());
            let dt = DataTransferBehaviour::new(peer_id, gs);
            let mut swarm = Swarm::new(trans, dt, peer_id);
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                swarm,
                store: bs,
            }
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
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

        async fn next(&mut self) -> Option<DataTransferEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    match event {
                        _ => return Some(event),
                    }
                } else {
                    println!("swarm: {:?}", ev);
                }
            }
        }
    }

    #[async_std::test]
    async fn test_pull() {
        let peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let store = peer1.store.clone();
        let pid1 = peer1.spawn("peer1");

        // generate 4MiB of random bytes
        const FILE_SIZE: usize = 4 * 1024 * 1024;
        let mut data = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data);

        let root = add(store.clone(), &data).unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let cid = root.unwrap();

        let id = peer2
            .swarm()
            .behaviour_mut()
            .pull(pid1, cid, selector, PullParams::default())
            .unwrap();

        loop {
            if let Some(event) = peer2.next().await {
                match event {
                    DataTransferEvent::Started(_) => {}
                    DataTransferEvent::Accepted(_) => {}
                    DataTransferEvent::Progress(_) => {}
                    DataTransferEvent::Block { .. } => {}
                    DataTransferEvent::Completed(chid, Ok(())) => {
                        assert_eq!(chid, id);
                        break;
                    }
                    e => {
                        panic!("unexpected event {:?}", e);
                    }
                }
            }
        }
    }

    #[async_std::test]
    async fn test_push() {
        use futures::join;

        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let store = peer2.store.clone();

        // generate 4MiB of random bytes
        const FILE_SIZE: usize = 4 * 1024 * 1024;
        let mut data = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data);

        let root = add(store.clone(), &data).unwrap();

        let selector = Selector::ExploreRecursive {
            limit: RecursionLimit::None,
            sequence: Box::new(Selector::ExploreAll {
                next: Box::new(Selector::ExploreRecursiveEdge),
            }),
            current: None,
        };

        let cid = root.unwrap();
        let pid1 = peer1.peer_id;

        // wait for both swarms to send a completed even in parallel
        join!(
            async {
                loop {
                    if let Some(event) = &peer1.next().await {
                        match event {
                            DataTransferEvent::Started(_) => {}
                            DataTransferEvent::Accepted(_) => {}
                            DataTransferEvent::Progress(_) => {}
                            DataTransferEvent::Block { .. } => {}
                            DataTransferEvent::Completed(_chid, Ok(())) => {
                                break;
                            }
                            e => {
                                panic!("unexpected event {:?}", e);
                            }
                        }
                    }
                }
            },
            async {
                let id = peer2
                    .swarm()
                    .behaviour_mut()
                    .push(pid1, cid, selector, PushParams::default())
                    .unwrap();

                loop {
                    if let Some(event) = peer2.next().await {
                        match event {
                            DataTransferEvent::Started(_) => {}
                            DataTransferEvent::Accepted(_) => {}
                            DataTransferEvent::Progress(_) => {}
                            DataTransferEvent::Block { .. } => {}
                            DataTransferEvent::Completed(chid, Ok(())) => {
                                assert_eq!(chid, id);
                                break;
                            }
                            e => {
                                panic!("unexpected event {:?}", e);
                            }
                        }
                    }
                }
            },
        );
        let pstore = peer1.store.clone();

        let buf = cat(pstore, cid).unwrap();
        assert_eq!(&buf, &data);
    }
}
