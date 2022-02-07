pub mod codec;
pub mod handler;

pub use codec::{MessageCodec, ProtocolName};

use handler::{OutboundProtocol, RequestResponseHandler, RequestResponseHandlerEvent};
use libp2p::core::{connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId};
use libp2p::swarm::{
    dial_opts::{self, DialOpts},
    DialError, IntoProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler,
    PollParameters,
};
use smallvec::SmallVec;
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
    time::Duration,
};

/// An inbound request or response.
#[derive(Debug)]
pub struct RequestResponseMessage<TMessage> {
    pub request_id: RequestId,
    pub message: TMessage,
}

/// The events emitted by a [`RequestResponse`] protocol.
#[derive(Debug)]
pub enum RequestResponseEvent<TMessage> {
    /// An incoming message (request or response).
    Message {
        /// The peer who sent the message.
        peer: PeerId,
        /// The incoming message.
        message: RequestResponseMessage<TMessage>,
    },
    /// An outbound request failed.
    OutboundFailure {
        /// The peer to whom the request was sent.
        peer: PeerId,
        /// The (local) ID of the failed request.
        request_id: RequestId,
        /// The error that occurred.
        error: OutboundFailure,
    },
    /// An inbound request failed.
    InboundFailure {
        /// The peer from whom the request was received.
        peer: PeerId,
        /// The ID of the failed inbound request.
        request_id: RequestId,
        /// The error that occurred.
        error: InboundFailure,
    },
}

/// Possible failures occurring in the context of sending
/// an outbound request and receiving the response.
#[derive(Debug, Clone, PartialEq)]
pub enum OutboundFailure {
    /// The request could not be sent because a dialing attempt failed.
    DialFailure,
    /// The request timed out before a response was received.
    ///
    /// It is not known whether the request may have been
    /// received (and processed) by the remote peer.
    Timeout,
    /// The connection closed before a response was received.
    ///
    /// It is not known whether the request may have been
    /// received (and processed) by the remote peer.
    ConnectionClosed,
    /// The remote supports none of the requested protocols.
    UnsupportedProtocols,
}

impl fmt::Display for OutboundFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutboundFailure::DialFailure => write!(f, "Failed to dial the requested peer"),
            OutboundFailure::Timeout => write!(f, "Timeout while waiting for a response"),
            OutboundFailure::ConnectionClosed => {
                write!(f, "Connection was closed before a response was received")
            }
            OutboundFailure::UnsupportedProtocols => {
                write!(f, "The remote supports none of the requested protocols")
            }
        }
    }
}

impl std::error::Error for OutboundFailure {}

/// Possible failures occurring in the context of receiving an
/// inbound request and sending a response.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundFailure {
    /// The inbound request timed out, either while reading the
    /// incoming request or before a response is sent, e.g. if
    /// [`RequestResponse::send_response`] is not called in a
    /// timely manner.
    Timeout,
    /// The connection closed before a response could be send.
    ConnectionClosed,
    /// The local peer supports none of the protocols requested
    /// by the remote.
    UnsupportedProtocols,
}

impl fmt::Display for InboundFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InboundFailure::Timeout => {
                write!(f, "Timeout while receiving request or sending response")
            }
            InboundFailure::ConnectionClosed => {
                write!(f, "Connection was closed before a response could be sent")
            }
            InboundFailure::UnsupportedProtocols => write!(
                f,
                "The local peer supports none of the protocols requested by the remote"
            ),
        }
    }
}

impl std::error::Error for InboundFailure {}

/// The ID of an inbound or outbound request.
///
/// Note: [`RequestId`]'s uniqueness is only guaranteed between two
/// inbound and likewise between two outbound requests. There is no
/// uniqueness guarantee in a set of both inbound and outbound
/// [`RequestId`]s nor in a set of inbound or outbound requests
/// originating from different [`RequestResponse`] behaviours.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(u64);

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The configuration for a `RequestResponse` protocol.
#[derive(Debug, Clone)]
pub struct RequestResponseConfig {
    request_timeout: Duration,
    connection_keep_alive: Duration,
}

impl Default for RequestResponseConfig {
    fn default() -> Self {
        Self {
            connection_keep_alive: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl RequestResponseConfig {
    /// Sets the keep-alive timeout of idle connections.
    pub fn set_connection_keep_alive(&mut self, v: Duration) -> &mut Self {
        self.connection_keep_alive = v;
        self
    }

    /// Sets the timeout for inbound and outbound requests.
    pub fn set_request_timeout(&mut self, v: Duration) -> &mut Self {
        self.request_timeout = v;
        self
    }
}

/// A request/response protocol for some message codec.
pub struct RequestResponse<TCodec>
where
    TCodec: MessageCodec + Clone + Send + 'static,
{
    /// The supported inbound protocols.
    inbound_protocols: SmallVec<[TCodec::Protocol; 2]>,
    /// The supported outbound protocols.
    outbound_protocols: SmallVec<[TCodec::Protocol; 2]>,
    /// The next (local) request ID.
    next_request_id: RequestId,
    /// The next (inbound) request ID.
    next_inbound_id: Arc<AtomicU64>,
    /// The protocol configuration.
    config: RequestResponseConfig,
    /// The protocol codec for reading and writing requests and responses.
    codec: TCodec,
    /// Pending events to return from `poll`.
    pending_events: VecDeque<
        NetworkBehaviourAction<
            RequestResponseEvent<TCodec::Message>,
            RequestResponseHandler<TCodec>,
        >,
    >,
    /// The currently connected peers, their pending outbound and inbound responses and their known,
    /// reachable addresses, if any.
    connected: HashMap<PeerId, SmallVec<[Connection; 2]>>,
    /// Externally managed addresses via `add_address` and `remove_address`.
    addresses: HashMap<PeerId, SmallVec<[Multiaddr; 6]>>,
    /// Requests that have not yet been sent and are waiting for a connection
    /// to be established.
    pending_outbound_requests: HashMap<PeerId, SmallVec<[OutboundProtocol<TCodec>; 10]>>,
}

impl<TCodec> RequestResponse<TCodec>
where
    TCodec: MessageCodec + Clone + Send + 'static,
{
    /// Creates a new `RequestResponse` behaviour for the given
    /// protocols, codec and configuration.
    pub fn new<I>(codec: TCodec, protocols: I, cfg: RequestResponseConfig) -> Self
    where
        I: IntoIterator<Item = TCodec::Protocol>,
    {
        let mut inbound_protocols = SmallVec::new();
        let mut outbound_protocols = SmallVec::new();
        for p in protocols {
            inbound_protocols.push(p.clone());
            outbound_protocols.push(p.clone());
        }
        RequestResponse {
            inbound_protocols,
            outbound_protocols,
            next_request_id: RequestId(1),
            next_inbound_id: Arc::new(AtomicU64::new(1)),
            config: cfg,
            codec,
            pending_events: VecDeque::new(),
            connected: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
            addresses: HashMap::new(),
        }
    }

    /// Initiates sending a request.
    ///
    /// If the targeted peer is currently not connected, a dialing
    /// attempt is initiated and the request is sent as soon as a
    /// connection is established.
    ///
    /// > **Note**: In order for such a dialing attempt to succeed,
    /// > the `RequestResonse` protocol must either be embedded
    /// > in another `NetworkBehaviour` that provides peer and
    /// > address discovery, or known addresses of peers must be
    /// > managed via [`RequestResponse::add_address`] and
    /// > [`RequestResponse::remove_address`].
    pub fn send_request(&mut self, peer: &PeerId, message: TCodec::Message) -> RequestId {
        let request_id = self.next_request_id();
        let request = OutboundProtocol {
            request_id,
            codec: self.codec.clone(),
            protocols: self.outbound_protocols.clone(),
            message,
        };

        if let Some(request) = self.try_send_request(peer, request) {
            let handler = self.new_handler();
            self.pending_events.push_back(NetworkBehaviourAction::Dial {
                opts: DialOpts::peer_id(*peer)
                    .condition(dial_opts::PeerCondition::Disconnected)
                    .build(),
                handler,
            });
            self.pending_outbound_requests
                .entry(*peer)
                .or_default()
                .push(request);
        }

        request_id
    }

    /// Adds a known address for a peer that can be used for
    /// dialing attempts by the `Swarm`, i.e. is returned
    /// by [`NetworkBehaviour::addresses_of_peer`].
    ///
    /// Addresses added in this way are only removed by `remove_address`.
    pub fn add_address(&mut self, peer: &PeerId, address: Multiaddr) {
        self.addresses.entry(*peer).or_default().push(address);
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
    }

    /// Checks whether a peer is currently connected.
    pub fn is_connected(&self, peer: &PeerId) -> bool {
        if let Some(connections) = self.connected.get(peer) {
            !connections.is_empty()
        } else {
            false
        }
    }

    /// Returns the next request ID.
    fn next_request_id(&mut self) -> RequestId {
        let request_id = self.next_request_id;
        self.next_request_id.0 += 1;
        request_id
    }

    /// Tries to send a request by queueing an appropriate event to be
    /// emitted to the `Swarm`. If the peer is not currently connected,
    /// the given request is return unchanged.
    fn try_send_request(
        &mut self,
        peer: &PeerId,
        request: OutboundProtocol<TCodec>,
    ) -> Option<OutboundProtocol<TCodec>> {
        if let Some(connections) = self.connected.get_mut(peer) {
            if connections.is_empty() {
                return Some(request);
            }
            let ix = (request.request_id.0 as usize) % connections.len();
            self.pending_events
                .push_back(NetworkBehaviourAction::NotifyHandler {
                    peer_id: *peer,
                    handler: NotifyHandler::One(connections[ix].id),
                    event: request,
                });
            None
        } else {
            Some(request)
        }
    }

    /// Returns a mutable reference to the connection in `self.connected`
    /// corresponding to the given [`PeerId`] and [`ConnectionId`].
    fn get_connection_mut(
        &mut self,
        peer: &PeerId,
        connection: ConnectionId,
    ) -> Option<&mut Connection> {
        self.connected
            .get_mut(peer)
            .and_then(|connections| connections.iter_mut().find(|c| c.id == connection))
    }
}

impl<TCodec> NetworkBehaviour for RequestResponse<TCodec>
where
    TCodec: MessageCodec + Send + Clone + 'static,
{
    type ProtocolsHandler = RequestResponseHandler<TCodec>;
    type OutEvent = RequestResponseEvent<TCodec::Message>;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        RequestResponseHandler::new(
            self.inbound_protocols.clone(),
            self.codec.clone(),
            self.config.connection_keep_alive,
            self.config.request_timeout,
            self.next_inbound_id.clone(),
        )
    }

    fn addresses_of_peer(&mut self, peer: &PeerId) -> Vec<Multiaddr> {
        let mut addresses = Vec::new();
        if let Some(connections) = self.connected.get(peer) {
            addresses.extend(connections.iter().filter_map(|c| c.address.clone()))
        }
        if let Some(more) = self.addresses.get(peer) {
            addresses.extend(more.into_iter().cloned());
        }
        addresses
    }

    fn inject_address_change(
        &mut self,
        peer: &PeerId,
        conn: &ConnectionId,
        _old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        let new_address = match new {
            ConnectedPoint::Dialer { address, .. } => Some(address.clone()),
            ConnectedPoint::Listener { .. } => None,
        };
        let connections = self
            .connected
            .get_mut(peer)
            .expect("Address change can only happen on an established connection.");

        let connection = connections
            .iter_mut()
            .find(|c| &c.id == conn)
            .expect("Address change can only happen on an established connection.");
        connection.address = new_address;
    }

    fn inject_connected(&mut self, peer: &PeerId) {
        if let Some(pending) = self.pending_outbound_requests.remove(peer) {
            for request in pending {
                let request = self.try_send_request(peer, request);
                assert!(request.is_none());
            }
        }
    }

    fn inject_connection_established(
        &mut self,
        peer: &PeerId,
        conn: &ConnectionId,
        endpoint: &ConnectedPoint,
        _errors: Option<&Vec<Multiaddr>>,
    ) {
        let address = match endpoint {
            ConnectedPoint::Dialer { address, .. } => Some(address.clone()),
            ConnectedPoint::Listener { .. } => None,
        };
        self.connected
            .entry(*peer)
            .or_default()
            .push(Connection::new(*conn, address));
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        conn: &ConnectionId,
        _: &ConnectedPoint,
        _: <Self::ProtocolsHandler as IntoProtocolsHandler>::Handler,
    ) {
        let connections = self
            .connected
            .get_mut(peer_id)
            .expect("Expected some established connection to peer before closing.");

        connections
            .iter()
            .position(|c| &c.id == conn)
            .map(|p: usize| connections.remove(p))
            .expect("Expected connection to be established before closing.");

        if connections.is_empty() {
            self.connected.remove(peer_id);
        }
    }

    fn inject_disconnected(&mut self, peer: &PeerId) {
        self.connected.remove(peer);
    }

    fn inject_dial_failure(
        &mut self,
        peer: Option<PeerId>,
        _: Self::ProtocolsHandler,
        _: &DialError,
    ) {
        if let Some(peer) = peer {
            // If there are pending outgoing requests when a dial failure occurs,
            // it is implied that we are not connected to the peer, since pending
            // outgoing requests are drained when a connection is established and
            // only created when a peer is not connected when a request is made.
            // Thus these requests must be considered failed, even if there is
            // another, concurrent dialing attempt ongoing.
            if let Some(pending) = self.pending_outbound_requests.remove(&peer) {
                for request in pending {
                    self.pending_events
                        .push_back(NetworkBehaviourAction::GenerateEvent(
                            RequestResponseEvent::OutboundFailure {
                                peer: peer,
                                request_id: request.request_id,
                                error: OutboundFailure::DialFailure,
                            },
                        ));
                }
            }
        }
    }

    fn inject_event(
        &mut self,
        peer: PeerId,
        connection: ConnectionId,
        event: RequestResponseHandlerEvent<TCodec>,
    ) {
        match event {
            RequestResponseHandlerEvent::Message {
                request_id,
                message,
            } => {
                let msg = RequestResponseMessage {
                    request_id,
                    message,
                };
                self.pending_events
                    .push_back(NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::Message { peer, message: msg },
                    ));

                match self.get_connection_mut(&peer, connection) {
                    Some(_connection) => {}
                    // Connection closed after `RequestResponseEvent::Request` has been emitted.
                    None => {
                        self.pending_events
                            .push_back(NetworkBehaviourAction::GenerateEvent(
                                RequestResponseEvent::InboundFailure {
                                    peer,
                                    request_id,
                                    error: InboundFailure::ConnectionClosed,
                                },
                            ));
                    }
                }
            }
            RequestResponseHandlerEvent::OutboundTimeout(request_id) => {
                self.pending_events
                    .push_back(NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::OutboundFailure {
                            peer,
                            request_id,
                            error: OutboundFailure::Timeout,
                        },
                    ));
            }
            RequestResponseHandlerEvent::InboundTimeout(request_id) => {
                self.pending_events
                    .push_back(NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::InboundFailure {
                            peer,
                            request_id,
                            error: InboundFailure::Timeout,
                        },
                    ));
            }
            RequestResponseHandlerEvent::OutboundUnsupportedProtocols(request_id) => {
                self.pending_events
                    .push_back(NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::OutboundFailure {
                            peer,
                            request_id,
                            error: OutboundFailure::UnsupportedProtocols,
                        },
                    ));
            }
            RequestResponseHandlerEvent::InboundUnsupportedProtocols(request_id) => {
                // Note: No need to call `self.remove_pending_outbound_response`,
                // `RequestResponseHandlerEvent::Request` was never emitted for this request and
                // thus request was never added to `pending_outbound_responses`.
                self.pending_events
                    .push_back(NetworkBehaviourAction::GenerateEvent(
                        RequestResponseEvent::InboundFailure {
                            peer,
                            request_id,
                            error: InboundFailure::UnsupportedProtocols,
                        },
                    ));
            }
        }
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        if let Some(ev) = self.pending_events.pop_front() {
            return Poll::Ready(ev);
        } else if self.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.pending_events.shrink_to_fit();
        }

        Poll::Pending
    }
}

/// Internal threshold for when to shrink the capacity
/// of empty queues. If the capacity of an empty queue
/// exceeds this threshold, the associated memory is
/// released.
const EMPTY_QUEUE_SHRINK_THRESHOLD: usize = 100;

/// Internal information tracked for an established connection.
struct Connection {
    id: ConnectionId,
    address: Option<Multiaddr>,
}

impl Connection {
    fn new(id: ConnectionId, address: Option<Multiaddr>) -> Self {
        Self { id, address }
    }
}
