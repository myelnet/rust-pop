use async_trait::async_trait;
use futures::prelude::*;
pub use libp2p::core::ProtocolName;
use std::io;

/// A `MessageCodec` defines a single message type that can be both used as
/// request and response types and how it's encoded / decoded on an I/O stream.
#[async_trait]
pub trait MessageCodec {
    /// The type of protocol(s) or protocol versions being negotiated.
    type Protocol: ProtocolName + Send + Clone;
    /// The type of inbound and outbound message.
    type Message: Send;

    /// Reads a message from the given I/O stream according to the
    /// negotiated protocol.
    async fn read_message<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Message>
    where
        T: AsyncRead + Unpin + Send;

    /// Writes a message to the given I/O stream according to the
    /// negotiated protocol.
    async fn write_message<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Message,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send;
}
