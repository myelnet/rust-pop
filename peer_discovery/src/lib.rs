mod request_manager;
mod response_manager;
mod types;

use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncWrite};
use futures::task::{Context, Poll};
use libp2p::request_response::{
    InboundFailure, OutboundFailure, ProtocolName, ProtocolSupport, RequestId as ReqResId,
    RequestResponse, RequestResponseCodec, RequestResponseConfig, RequestResponseEvent,
    RequestResponseMessage,
};
use request_manager::{RequestEvent, RequestManager};
use response_manager::{ResponseEvent, ResponseManager};
// use futures::{future::BoxFuture, prelude::*, stream::FuturesUnordered};
use libp2p::core::connection::{ConnectionId, ListenerId};
use libp2p::core::{upgrade, ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::{
    DialError, IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
    ProtocolsHandler,
};
// use protobuf::Error as PBMessage;
// use protobuf::Message as PBMessage;
use async_std::io;
use serde::{Deserialize, Serialize};
use serde_cbor::{from_slice, to_vec};
use std::collections::HashMap;
use std::error::Error;
// use std::io;
use futures::prelude::*;
use smallvec::SmallVec;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

pub type RequestId = i32;
pub type PeerTable = HashMap<PeerId, SmallVec<[Multiaddr; 6]>>;
pub type SerializablePeerTable = HashMap<Vec<u8>, Vec<Vec<u8>>>;

#[derive(Debug, Clone)]
pub struct PeerDiscoveryProtocol;

impl ProtocolName for PeerDiscoveryProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/myel/peer-discovery/0.1"
    }
}

#[derive(Clone)]
pub struct DiscoveryCodec {
    max_msg_size: usize,
}

impl Default for DiscoveryCodec {
    fn default() -> Self {
        Self {
            max_msg_size: 1000000,
        }
    }
}

#[async_trait]
impl RequestResponseCodec for DiscoveryCodec {
    type Protocol = PeerDiscoveryProtocol;
    type Response = DiscoveryResponse;
    type Request = DiscoveryRequest;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Send + Unpin,
    {
        let packet = upgrade::read_length_prefixed(io, self.max_msg_size)
            .await
            .map_err(other)?;
        if packet.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "End of output"));
        }

        let message = from_slice(&packet).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(message)
    }
    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        request: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        let bytes = to_vec(&request).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        upgrade::write_length_prefixed(io, bytes).await?;
        Ok(())
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = upgrade::read_length_prefixed(io, self.max_msg_size).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        let message = from_slice(&vec).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(message)
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let bytes = to_vec(&res).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        upgrade::write_length_prefixed(io, bytes).await?;

        io.close().await?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum DiscoveryEvent {
    RequestAccepted(PeerId, DiscoveryRequest),
    ResponseCompleted(PeerId, RequestId),
    // We may receive more than one response at once
    ResponseReceived(DiscoveryResponse),
    Complete(RequestId),
}

pub type IncomingRequestHook = Arc<dyn Fn(&PeerId) -> bool + Send + Sync>;

pub struct DiscoveryHooks {
    incoming_request: IncomingRequestHook,
}

impl Default for DiscoveryHooks {
    fn default() -> Self {
        Self {
            incoming_request: Arc::new(|_| true),
        }
    }
}

impl DiscoveryHooks {
    pub fn register_incoming_request(&mut self, f: IncomingRequestHook) {
        self.incoming_request = f;
    }
}

#[derive(Clone)]
pub struct Config {
    /// Timeout of a request.
    pub request_timeout: Duration,
    /// Time a connection is kept alive.
    pub connection_keep_alive: Duration,
}

impl Config {
    pub fn new() -> Self {
        Self {
            request_timeout: Duration::from_secs(10 * 60),
            connection_keep_alive: Duration::from_secs(10 * 60),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

pub struct PeerDiscovery {
    inner: RequestResponse<DiscoveryCodec>,
    request_manager: RequestManager,
    response_manager: ResponseManager,
    hooks: Arc<RwLock<DiscoveryHooks>>,
    addresses: PeerTable,
}

impl PeerDiscovery {
    pub fn new(config: Config) -> Self {
        let protocols = std::iter::once((PeerDiscoveryProtocol, ProtocolSupport::Full));
        let mut rr_config = RequestResponseConfig::default();
        rr_config.set_connection_keep_alive(config.connection_keep_alive);
        rr_config.set_request_timeout(config.request_timeout);
        let inner = RequestResponse::new(DiscoveryCodec::default(), protocols, rr_config);
        let hooks = Arc::new(RwLock::new(DiscoveryHooks::default()));
        Self {
            inner,
            request_manager: RequestManager::new(),
            response_manager: ResponseManager::new(hooks.clone()),
            hooks,
            addresses: HashMap::new(),
        }
    }
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.addresses
            .entry(*peer)
            .or_default()
            .push(address.clone());
        self.inner.add_address(peer, address);
    }

    /// Removes an address of a peer previously added via `add_address`.
    pub fn remove_address(&mut self, peer: &PeerId, address: &Multiaddr) {
        let mut last = false;
        if let Some(addresses) = self.addresses.get_mut(peer) {
            addresses.retain(|a| a != address);
            last = addresses.is_empty();
        }
        if last {
            self.addresses.remove(peer);
        }
        self.inner.remove_address(peer, address);
    }

    pub fn request(&mut self, peer_id: PeerId) -> RequestId {
        self.request_manager.start_request(peer_id)
    }
    fn inject_outbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: ReqResId,
        error: &OutboundFailure,
    ) {
        println!(
            "peer discovery outbound failure {} {} {:?}",
            peer, request_id, error
        );
    }
    fn inject_inbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: ReqResId,
        error: &InboundFailure,
    ) {
        println!(
            "inbound peer discovery failure {} {} {:?}",
            peer, request_id, error
        );
    }
    pub fn register_incoming_request_hook(&mut self, hook: IncomingRequestHook) {
        self.hooks.write().unwrap().register_incoming_request(hook);
    }
}

impl NetworkBehaviour for PeerDiscovery {
    type ProtocolsHandler = <RequestResponse<DiscoveryCodec> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = DiscoveryEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.inner.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.inner.addresses_of_peer(peer_id)
    }

    fn inject_connected(&mut self, peer_id: &PeerId) {
        self.inner.inject_connected(peer_id)
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId) {
        self.inner.inject_disconnected(peer_id);
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        failed_addressses: Option<&Vec<Multiaddr>>,
    ) {
        self.inner
            .inject_connection_established(peer_id, conn, endpoint, failed_addressses)
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        handler: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
    ) {
        let req_res = handler;
        self.inner
            .inject_connection_closed(peer_id, conn, endpoint, req_res)
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
        error: &DialError,
    ) {
        let req_res = handler;
        self.inner.inject_dial_failure(peer_id, req_res, error)
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.inner.inject_new_listen_addr(id, addr)
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.inner.inject_expired_listen_addr(id, addr)
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_new_external_addr(addr)
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        self.inner.inject_expired_external_addr(addr)
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn Error + 'static)) {
        self.inner.inject_listener_error(id, err)
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.inner.inject_listener_closed(id, reason)
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        return self.inner.inject_event(peer_id, conn, event);
    }
    fn poll(
        &mut self,
        ctx: &mut Context,
        pparams: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        let mut exit = false;
        while !exit {
            exit = true;
            while let Poll::Ready(Some(event)) = self.response_manager.next(ctx) {
                exit = false;
                match event {
                    ResponseEvent::Accepted(peer, req) => {
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            DiscoveryEvent::RequestAccepted(peer, req),
                        ));
                    }
                    ResponseEvent::Completed(peer, chan, msg) => {
                        let id = msg.id.clone();
                        let addresses = &self.addresses;
                        self.inner.send_response(chan, msg);
                        return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                            DiscoveryEvent::ResponseCompleted(peer, id),
                        ));
                    }
                }
            }
            while let Poll::Ready(Some(event)) = self.request_manager.next(ctx) {
                exit = false;
                match event {
                    RequestEvent::NewRequest(responder, req) => {
                        self.inner.send_request(&responder, req);
                    }
                }
            }
            while let Poll::Ready(event) = self.inner.poll(ctx, pparams) {
                exit = false;
                let event = match event {
                    NetworkBehaviourAction::GenerateEvent(event) => event,
                    NetworkBehaviourAction::Dial { opts, handler } => {
                        return Poll::Ready(NetworkBehaviourAction::Dial { opts, handler });
                    }
                    NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        handler,
                        event,
                    } => {
                        return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                            peer_id,
                            handler,
                            event,
                        });
                    }
                    NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                        return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                            address,
                            score,
                        });
                    }
                    NetworkBehaviourAction::CloseConnection {
                        peer_id,
                        connection,
                    } => {
                        return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                            peer_id,
                            connection,
                        });
                    }
                };
                match event {
                    RequestResponseEvent::Message { peer, message } => match message {
                        RequestResponseMessage::Request {
                            request_id: _,
                            request,
                            channel,
                        } => {
                            self.response_manager.inject_request(
                                peer,
                                &request,
                                channel,
                                self.addresses.clone(),
                            );
                        }
                        RequestResponseMessage::Response {
                            request_id: _,
                            response,
                        } => {
                            //  addresses only get returned on a successful response
                            match response.clone().addresses {
                                Some(addresses) => {
                                    let new_addresses: PeerTable = addresses
                                        .iter()
                                        .map_while(|(peer, addresses)| {
                                            let mut addr_vec = SmallVec::<[Multiaddr; 6]>::new();
                                            for addr in addresses {
                                                addr_vec.push(
                                                    Multiaddr::try_from(addr.clone()).unwrap(),
                                                )
                                            }
                                            Some((PeerId::from_bytes(peer).unwrap(), addr_vec))
                                        })
                                        .collect();
                                    //  update our local peer table
                                    self.addresses.extend(new_addresses);
                                }
                                None => {}
                            }
                            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(
                                DiscoveryEvent::ResponseReceived(response),
                            ));
                        }
                    },
                    RequestResponseEvent::OutboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        self.inject_outbound_failure(&peer, request_id, &error);
                    }
                    RequestResponseEvent::InboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        self.inject_inbound_failure(&peer, request_id, &error);
                    }
                    RequestResponseEvent::ResponseSent {
                        peer: _,
                        request_id: _,
                    } => {}
                }
            }
        }
        Poll::Pending
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DiscoveryRequest {
    pub id: RequestId,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DiscoveryResponse {
    pub id: RequestId,
    pub status: ResponseStatusCode,
    pub addresses: Option<SerializablePeerTable>,
}

#[derive(PartialEq, Clone, Copy, Eq, Debug, Serialize, Deserialize)]
pub enum ResponseStatusCode {
    RequestAcknowledged,
    PartialResponse,
    RequestPaused,
    RequestCompletedFull,
    RequestRejected,
    RequestFailedBusy,
    RequestFailedUnknown,
    RequestFailedLegal,
    RequestFailedPeersNotFound,
    RequestCancelled,
    Other(i32),
}

impl ResponseStatusCode {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::RequestAcknowledged => 10,
            Self::PartialResponse => 14,
            Self::RequestPaused => 15,
            Self::RequestCompletedFull => 20,
            Self::RequestRejected => 30,
            Self::RequestFailedBusy => 31,
            Self::RequestFailedUnknown => 32,
            Self::RequestFailedLegal => 33,
            Self::RequestFailedPeersNotFound => 34,
            Self::RequestCancelled => 35,
            Self::Other(code) => code,
        }
    }
    pub fn from_i32(code: i32) -> Self {
        match code {
            10 => Self::RequestAcknowledged,
            14 => Self::PartialResponse,
            15 => Self::RequestPaused,
            20 => Self::RequestCompletedFull,
            30 => Self::RequestRejected,
            31 => Self::RequestFailedBusy,
            32 => Self::RequestFailedUnknown,
            33 => Self::RequestFailedLegal,
            34 => Self::RequestFailedPeersNotFound,
            35 => Self::RequestCancelled,
            _ => Self::Other(code),
        }
    }
}

fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}
