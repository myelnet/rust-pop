mod fsm;
mod network;

pub use network::DealParams;

use blockstore::types::BlockStore;
use filecoin::{cid_helpers::CidCbor, types::Cbor};
use fsm::{Channel, ChannelEvent};
use graphsync::traversal::{RecursionLimit, Selector};
use graphsync::{Extensions, Graphsync, GraphsyncEvent, RequestId};
use instant;
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use libp2p::swarm::{
    NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
};
use libp2p::{Multiaddr, NetworkBehaviour, PeerId};
use network::{
    ChannelId, DataTransferNetwork, DealProposal, DealResponse, DealStatus, DtNetEvent,
    MessageType, TransferMessage, TransferRequest, TransferResponse, EMPTY_QUEUE_SHRINK_THRESHOLD,
    EXTENSION_KEY,
};
use num_bigint::ToBigInt;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug)]
pub enum DataTransferEvent {
    Progress(ChannelId),
    Completed(ChannelId, Result<(), String>),
}

#[derive(NetworkBehaviour)]
#[behaviour(
    out_event = "DataTransferEvent",
    poll_method = "poll",
    event_process = true
)]
pub struct DataTransfer<S: BlockStore>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    graphsync: Graphsync<S>,
    network: DataTransferNetwork,

    #[behaviour(ignore)]
    peer_id: PeerId,
    #[behaviour(ignore)]
    next_request_id: u64,
    #[behaviour(ignore)]
    pending_events: VecDeque<DataTransferEvent>,
    #[behaviour(ignore)]
    channels: HashMap<ChannelId, Channel>,
    #[behaviour(ignore)]
    channel_ids: HashMap<RequestId, ChannelId>,
}

impl<S: 'static + BlockStore> DataTransfer<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    pub fn new(peer_id: PeerId, mut graphsync: Graphsync<S>) -> Self {
        let now_unix = instant::now() as u64;

        graphsync.register_incoming_request_hook(Arc::new(|_peer, gs_req| {
            let mut extensions = HashMap::new();
            if let Ok(rmsg) = TransferMessage::try_from(&gs_req.extensions) {
                let req = rmsg
                    .request
                    .expect("Expected incoming request to have a request");
                let vch = req.voucher.expect("Expected request to have a voucher");
                let voucher = DealResponse {
                    id: vch.id,
                    status: DealStatus::Accepted,
                    message: "".to_string(),
                    payment_owed: (0 as isize).to_bigint().unwrap(),
                };
                let tmsg = TransferMessage {
                    is_rq: false,
                    request: None,
                    response: Some(TransferResponse {
                        mtype: MessageType::NewMessage,
                        accepted: true,
                        paused: false,
                        transfer_id: req.transfer_id,
                        voucher: Some(voucher),
                        voucher_type: DealResponse::voucher_type(),
                    }),
                };
                extensions.insert(EXTENSION_KEY.to_string(), tmsg.marshal_cbor().unwrap());
                (true, extensions)
            } else {
                (false, extensions)
            }
        }));
        Self {
            peer_id,
            graphsync,
            next_request_id: now_unix,
            network: DataTransferNetwork::new(),
            pending_events: VecDeque::default(),
            channel_ids: HashMap::new(),
            channels: HashMap::new(),
        }
    }

    pub fn pull(
        &mut self,
        peer_id: PeerId,
        root: Cid,
        selector: Selector,
        params: DealParams,
    ) -> Result<ChannelId, String> {
        let cid = CidCbor::from(root.clone());
        let request_id = self.next_request_id();
        let voucher = DealProposal {
            id: request_id,
            payload_cid: cid.clone(),
            params,
        };
        let message = TransferMessage {
            is_rq: true,
            request: Some(TransferRequest {
                root: cid,
                mtype: MessageType::NewMessage,
                pause: false,
                partial: false,
                pull: true,
                selector: selector.clone(),
                voucher: Some(voucher),
                voucher_type: DealProposal::voucher_type(),
                transfer_id: request_id,
                restart_channel: Default::default(),
            }),
            response: None,
        };
        let buf = message.marshal_cbor().map_err(|e| e.to_string())?;
        let mut extensions = HashMap::new();
        extensions.insert(EXTENSION_KEY.to_string(), buf);
        let rq_id = self.graphsync.request(peer_id, root, selector, extensions);
        let ch_id = self.channel_id(request_id, peer_id);
        self.channel_ids.insert(rq_id, ch_id.clone());
        self.channels.insert(
            ch_id.clone(),
            Channel::New {
                id: ch_id.clone(),
                deal_id: request_id,
            },
        );
        Ok(ch_id)
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

    fn poll(
        &mut self,
        cx: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self as NetworkBehaviour>::OutEvent,
            <Self as NetworkBehaviour>::ProtocolsHandler,
        >,
    > {
        if let Some(ev) = self.pending_events.pop_front() {
            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(ev));
        } else if self.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.pending_events.shrink_to_fit();
        }
        Poll::Pending
    }

    fn get_channel_by_req_id(&mut self, req_id: RequestId) -> Option<(ChannelId, Channel)> {
        let ch_id = self.channel_ids.get(&req_id)?;
        let ch = self.channels.remove(&ch_id)?;
        Some((ch_id.clone(), ch))
    }

    fn process_request(&mut self, peer: PeerId, request: TransferRequest) -> ChannelId {
        let ch_id = self.channel_id(request.transfer_id, peer);
        self.channels.insert(
            ch_id.clone(),
            Channel::New {
                id: ch_id.clone(),
                deal_id: request
                    .voucher
                    .expect("Expected request to contain voucher")
                    .id,
            },
        );
        ch_id
    }

    fn process_response(&mut self, peer: PeerId, response: TransferResponse) {
        let ch_id = self.channel_id(response.transfer_id, peer);
        if response.accepted {
            if let Some(voucher) = response.voucher {
                match voucher.status {
                    DealStatus::Completed => {
                        let ch = self
                            .channels
                            .remove(&ch_id)
                            .expect("Expected channel to be created before response");
                        let next_state = ch.transition(ChannelEvent::Completed);
                        self.channels.insert(ch_id, next_state.clone());
                        self.pending_events.push_back(next_state.into());
                    }
                    _ => {}
                }
            }
        } else {
            unimplemented!("TODO");
        }
    }

    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.graphsync.add_address(peer_id, addr);
    }
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<GraphsyncEvent> for DataTransfer<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: GraphsyncEvent) {
        match event {
            GraphsyncEvent::RequestAccepted(peer, req) => {
                if let Ok(msg) = TransferMessage::try_from(&req.extensions) {
                    let ch_id = self.process_request(
                        peer,
                        msg.request
                            .expect("Expected graphsync request to contain request"),
                    );
                    self.channel_ids.insert(req.id, ch_id);
                }
            }
            GraphsyncEvent::ResponseCompleted(peer, req_ids) => {
                for id in req_ids {
                    let (ch_id, ch) = self.get_channel_by_req_id(id).expect(
                        "Expected channel to be created before graphsync response is completed",
                    );

                    if let Channel::New { deal_id, .. } = ch {
                        let voucher = DealResponse {
                            id: deal_id,
                            status: DealStatus::Completed,
                            message: "Thanks for doing business with us".to_string(),
                            payment_owed: (0 as isize).to_bigint().unwrap(),
                        };
                        let tmsg = TransferMessage {
                            is_rq: false,
                            request: None,
                            response: Some(TransferResponse {
                                mtype: MessageType::CompleteMessage,
                                accepted: true,
                                paused: false,
                                transfer_id: ch_id.id,
                                voucher: Some(voucher),
                                voucher_type: DealResponse::voucher_type(),
                            }),
                        };
                        self.network.send_message(&peer, tmsg);
                    }
                }
            }
            GraphsyncEvent::ResponseReceived(responses) => {
                for res in responses.iter() {
                    match TransferMessage::try_from(&res.extensions) {
                        Ok(res) => println!("received response {:?}", res),
                        Err(e) => {}
                    };
                }
            }
            GraphsyncEvent::Progress { req_id, link, size } => {
                let (ch_id, ch) = self
                    .get_channel_by_req_id(req_id)
                    .expect("Expected channel to be created before graphsync progress");
                let event = ChannelEvent::BlockReceived { size };
                let next_state = ch.transition(event);
                self.channels.insert(ch_id.clone(), next_state.clone());
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
}

impl<S: 'static + BlockStore> NetworkBehaviourEventProcess<DtNetEvent> for DataTransfer<S>
where
    Ipld: Decode<<S::Params as StoreParams>::Codecs>,
{
    fn inject_event(&mut self, event: DtNetEvent) {
        match event {
            DtNetEvent::Request(peer_id, request) => {
                self.process_request(peer_id, request);
            }
            DtNetEvent::Response(peer_id, response) => {
                self.process_response(peer_id, response);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use blockstore::memory::MemoryDB as MemoryBlockStore;
    use dag_service::{add, cat};
    use futures::prelude::*;
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
        swarm: Swarm<DataTransfer<MemoryBlockStore>>,
        store: Arc<MemoryBlockStore>,
    }

    impl Peer {
        fn new() -> Self {
            let (peer_id, trans) = mk_transport();
            let bs = Arc::new(MemoryBlockStore::default());
            let gs = Graphsync::new(Default::default(), bs.clone());
            let dt = DataTransfer::new(peer_id, gs);
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

        fn swarm(&mut self) -> &mut Swarm<DataTransfer<MemoryBlockStore>> {
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
                    return Some(event);
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

        // generate 4MiB of random bytes
        const FILE_SIZE: usize = 4 * 1024 * 1024;
        let mut data = vec![0u8; FILE_SIZE];
        rand::thread_rng().fill_bytes(&mut data);

        let root = add(store.clone(), &data).unwrap();

        let peer1 = peer1.spawn("peer1");

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
            .pull(peer1, cid, selector, DealParams::default())
            .unwrap();

        loop {
            if let Some(event) = peer2.next().await {
                match event {
                    DataTransferEvent::Progress(chi) => {}
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
}
