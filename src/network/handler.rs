use crate::network::codec::MessageCodec;
use crate::network::{RequestId, EMPTY_QUEUE_SHRINK_THRESHOLD};
use crossbeam_channel::{bounded, Receiver, Sender};
use futures::{future::BoxFuture, prelude::*};
use futures_lite::stream::StreamExt;
use instant::Instant;
use libp2p::core::upgrade::{
    InboundUpgrade, NegotiationError, OutboundUpgrade, UpgradeError, UpgradeInfo,
};
use libp2p::swarm::{
    protocols_handler::{
        KeepAlive, ProtocolsHandler, ProtocolsHandlerEvent, ProtocolsHandlerUpgrErr,
    },
    NegotiatedSubstream, SubstreamProtocol,
};
use smallvec::SmallVec;
use std::{
    collections::VecDeque,
    fmt, io,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

/// Inbound message substream upgrade protocol.
///
/// Receives message.
#[derive(Debug)]
pub struct InboundProtocol<TCodec>
where
    TCodec: MessageCodec,
{
    pub(crate) codec: TCodec,
    pub(crate) protocols: SmallVec<[TCodec::Protocol; 2]>,
    pub(crate) sender: Sender<(RequestId, TCodec::Message)>,
    pub(crate) request_id: RequestId,
}

impl<TCodec> UpgradeInfo for InboundProtocol<TCodec>
where
    TCodec: MessageCodec,
{
    type Info = TCodec::Protocol;
    type InfoIter = smallvec::IntoIter<[Self::Info; 2]>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.protocols.clone().into_iter()
    }
}

impl<TCodec> InboundUpgrade<NegotiatedSubstream> for InboundProtocol<TCodec>
where
    TCodec: MessageCodec + Send + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(
        mut self,
        mut io: NegotiatedSubstream,
        protocol: Self::Info,
    ) -> Self::Future {
        async move {
            while let Ok(msg) = self.codec.read_message(&protocol, &mut io).await {
                match self.sender.send((self.request_id, msg)) {
                    Ok(()) => {}
                    Err(_) => panic!(
                        "Expect request receiver to be alive i.e. protocol handler to be alive.",
                    ),
                }
            }
            io.close().await?;
            Ok(())
        }
        .boxed()
    }
}

/// Outbound substream upgrade protocol for responses.
///
/// Sends a message.
pub struct OutboundProtocol<TCodec>
where
    TCodec: MessageCodec,
{
    pub(crate) codec: TCodec,
    pub(crate) protocols: SmallVec<[TCodec::Protocol; 2]>,
    pub(crate) request_id: RequestId,
    pub(crate) receiver: Option<Receiver<TCodec::Message>>,
    pub(crate) message: TCodec::Message,
}

impl<TCodec> fmt::Debug for OutboundProtocol<TCodec>
where
    TCodec: MessageCodec,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundProtocol")
            .field("request_id", &self.request_id)
            .finish()
    }
}

impl<TCodec> UpgradeInfo for OutboundProtocol<TCodec>
where
    TCodec: MessageCodec,
{
    type Info = TCodec::Protocol;
    type InfoIter = smallvec::IntoIter<[Self::Info; 2]>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.protocols.clone().into_iter()
    }
}

impl<TCodec> OutboundUpgrade<NegotiatedSubstream> for OutboundProtocol<TCodec>
where
    TCodec: MessageCodec + Send + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(
        mut self,
        mut io: NegotiatedSubstream,
        protocol: Self::Info,
    ) -> Self::Future {
        async move {
            let write = self.codec.write_message(&protocol, &mut io, self.message);
            write.await?;

            if let Some(r) = self.receiver {
                while let Ok(msg) = r.recv() {
                    let write = self.codec.write_message(&protocol, &mut io, msg);
                    write.await?;
                }
            }
            io.close().await?;
            Ok(())
        }
        .boxed()
    }
}

/// A connection handler of a `RequestResponse` protocol.
#[doc(hidden)]
pub struct RequestResponseHandler<TCodec>
where
    TCodec: MessageCodec,
{
    /// The supported inbound protocols.
    inbound_protocols: SmallVec<[TCodec::Protocol; 2]>,
    /// The request/response message codec.
    codec: TCodec,
    /// The keep-alive timeout of idle connections. A connection is considered
    /// idle if there are no outbound substreams.
    keep_alive_timeout: Duration,
    /// The timeout for inbound and outbound substreams (i.e. request
    /// and response processing).
    substream_timeout: Duration,
    /// The current connection keep-alive.
    keep_alive: KeepAlive,
    /// A pending fatal error that results in the connection being closed.
    pending_error: Option<ProtocolsHandlerUpgrErr<io::Error>>,
    /// Queue of events to emit in `poll()`.
    pending_events: VecDeque<RequestResponseHandlerEvent<TCodec>>,
    /// Outbound upgrades waiting to be emitted as an `OutboundSubstreamRequest`.
    outbound: VecDeque<OutboundProtocol<TCodec>>,
    /// Inbound upgrades waiting for the incoming request.
    inbound: Receiver<(RequestId, TCodec::Message)>,
    inbound_send: Sender<(RequestId, TCodec::Message)>,
    inbound_request_id: Arc<AtomicU64>,
}

impl<TCodec> RequestResponseHandler<TCodec>
where
    TCodec: MessageCodec,
{
    pub(super) fn new(
        inbound_protocols: SmallVec<[TCodec::Protocol; 2]>,
        codec: TCodec,
        keep_alive_timeout: Duration,
        substream_timeout: Duration,
        inbound_request_id: Arc<AtomicU64>,
    ) -> Self {
        let (s, r) = bounded(1000);
        Self {
            inbound_protocols,
            codec,
            keep_alive: KeepAlive::Yes,
            keep_alive_timeout,
            substream_timeout,
            outbound: VecDeque::new(),
            inbound: r,
            inbound_send: s,
            pending_events: VecDeque::new(),
            pending_error: None,
            inbound_request_id,
        }
    }
}

/// The events emitted by the [`RequestResponseHandler`].
#[doc(hidden)]
pub enum RequestResponseHandlerEvent<TCodec>
where
    TCodec: MessageCodec,
{
    /// A message has been received.
    Message {
        request_id: RequestId,
        message: TCodec::Message,
    },
    /// An outbound request timed out while sending the request
    /// or waiting for the response.
    OutboundTimeout(RequestId),
    /// An outbound request failed to negotiate a mutually supported protocol.
    OutboundUnsupportedProtocols(RequestId),
    /// An inbound request timed out while waiting for the request
    /// or sending the response.
    InboundTimeout(RequestId),
    /// An inbound request failed to negotiate a mutually supported protocol.
    InboundUnsupportedProtocols(RequestId),
}

impl<TCodec: MessageCodec> fmt::Debug for RequestResponseHandlerEvent<TCodec> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestResponseHandlerEvent::Message {
                request_id,
                message: _,
            } => f
                .debug_struct("RequestResponseHandlerEvent::Message")
                .field("request_id", request_id)
                .finish(),
            RequestResponseHandlerEvent::OutboundTimeout(request_id) => f
                .debug_tuple("RequestResponseHandlerEvent::OutboundTimeout")
                .field(request_id)
                .finish(),
            RequestResponseHandlerEvent::OutboundUnsupportedProtocols(request_id) => f
                .debug_tuple("RequestResponseHandlerEvent::OutboundUnsupportedProtocols")
                .field(request_id)
                .finish(),
            RequestResponseHandlerEvent::InboundTimeout(request_id) => f
                .debug_tuple("RequestResponseHandlerEvent::InboundTimeout")
                .field(request_id)
                .finish(),
            RequestResponseHandlerEvent::InboundUnsupportedProtocols(request_id) => f
                .debug_tuple("RequestResponseHandlerEvent::InboundUnsupportedProtocols")
                .field(request_id)
                .finish(),
        }
    }
}

impl<TCodec> ProtocolsHandler for RequestResponseHandler<TCodec>
where
    TCodec: MessageCodec + Send + Clone + 'static,
{
    type InEvent = OutboundProtocol<TCodec>;
    type OutEvent = RequestResponseHandlerEvent<TCodec>;
    type Error = ProtocolsHandlerUpgrErr<io::Error>;
    type InboundProtocol = InboundProtocol<TCodec>;
    type OutboundProtocol = OutboundProtocol<TCodec>;
    type OutboundOpenInfo = RequestId;
    type InboundOpenInfo = RequestId;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        let request_id = RequestId(self.inbound_request_id.fetch_add(1, Ordering::Relaxed));

        // By keeping all I/O inside the `ResponseProtocol` and thus the
        // inbound substream upgrade via above channels, we ensure that it
        // is all subject to the configured timeout without extra bookkeeping
        // for inbound substreams as well as their timeouts and also make the
        // implementation of inbound and outbound upgrades symmetric in
        // this sense.
        let proto = InboundProtocol {
            protocols: self.inbound_protocols.clone(),
            codec: self.codec.clone(),
            sender: self.inbound_send.clone(),
            request_id,
        };

        SubstreamProtocol::new(proto, request_id).with_timeout(self.substream_timeout)
    }

    fn inject_fully_negotiated_inbound(&mut self, sent: (), request_id: RequestId) {
        self.keep_alive = KeepAlive::Yes;
        // self.inbound.push_back((request_id, sent));
    }

    fn inject_fully_negotiated_outbound(&mut self, _result: (), _request_id: RequestId) {}

    fn inject_event(&mut self, request: Self::InEvent) {
        self.keep_alive = KeepAlive::Yes;
        self.outbound.push_back(request);
    }

    fn inject_dial_upgrade_error(
        &mut self,
        info: RequestId,
        error: ProtocolsHandlerUpgrErr<io::Error>,
    ) {
        match error {
            ProtocolsHandlerUpgrErr::Timeout => {
                self.pending_events
                    .push_back(RequestResponseHandlerEvent::OutboundTimeout(info));
            }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)) => {
                // The remote merely doesn't support the protocol(s) we requested.
                // This is no reason to close the connection, which may
                // successfully communicate with other protocols already.
                // An event is reported to permit user code to react to the fact that
                // the remote peer does not support the requested protocol(s).
                self.pending_events.push_back(
                    RequestResponseHandlerEvent::OutboundUnsupportedProtocols(info),
                );
            }
            _ => {
                // Anything else is considered a fatal error or misbehaviour of
                // the remote peer and results in closing the connection.
                self.pending_error = Some(error);
            }
        }
    }

    fn inject_listen_upgrade_error(
        &mut self,
        info: RequestId,
        error: ProtocolsHandlerUpgrErr<io::Error>,
    ) {
        match error {
            ProtocolsHandlerUpgrErr::Timeout => self
                .pending_events
                .push_back(RequestResponseHandlerEvent::InboundTimeout(info)),
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)) => {
                // The local peer merely doesn't support the protocol(s) requested.
                // This is no reason to close the connection, which may
                // successfully communicate with other protocols already.
                // An event is reported to permit user code to react to the fact that
                // the local peer does not support the requested protocol(s).
                self.pending_events.push_back(
                    RequestResponseHandlerEvent::InboundUnsupportedProtocols(info),
                );
            }
            _ => {
                // Anything else is considered a fatal error or misbehaviour of
                // the remote peer and results in closing the connection.
                self.pending_error = Some(error);
            }
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<ProtocolsHandlerEvent<OutboundProtocol<TCodec>, RequestId, Self::OutEvent, Self::Error>>
    {
        // Check for a pending (fatal) error.
        if let Some(err) = self.pending_error.take() {
            // The handler will not be polled again by the `Swarm`.
            return Poll::Ready(ProtocolsHandlerEvent::Close(err));
        }

        // Drain pending events.
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(ProtocolsHandlerEvent::Custom(event));
        } else if self.pending_events.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.pending_events.shrink_to_fit();
        }

        // Check for inbound requests.
        if let Ok((id, message)) = self.inbound.try_recv() {
            return Poll::Ready(ProtocolsHandlerEvent::Custom(
                RequestResponseHandlerEvent::Message {
                    request_id: id,
                    message,
                },
            ));
        }

        // Emit outbound requests.
        if let Some(request) = self.outbound.pop_front() {
            let info = request.request_id;
            return Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(request, info)
                    .with_timeout(self.substream_timeout),
            });
        }

        debug_assert!(self.outbound.is_empty());

        if self.outbound.capacity() > EMPTY_QUEUE_SHRINK_THRESHOLD {
            self.outbound.shrink_to_fit();
        }

        if self.inbound.is_empty() && self.keep_alive.is_yes() {
            // No new inbound or outbound requests. However, we may just have
            // started the latest inbound or outbound upgrade(s), so make sure
            // the keep-alive timeout is preceded by the substream timeout.
            let until = Instant::now() + self.substream_timeout + self.keep_alive_timeout;
            self.keep_alive = KeepAlive::Until(until);
        }

        Poll::Pending
    }
}
