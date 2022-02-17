use super::{DiscoveryResponse, RequestId, ResponseStatusCode, SerializablePeerTable};
use serde_cbor::error::Error as CborError;
use std::io::Error as StdError;
use thiserror::Error;

#[derive(Debug, PartialEq, Error)]
pub enum Error {
    #[error("{0}")]
    Encoding(String),
    #[error("{0}")]
    Custom(String),
}

impl From<StdError> for Error {
    fn from(err: StdError) -> Error {
        Self::Custom(err.to_string())
    }
}

impl From<CborError> for Error {
    fn from(err: CborError) -> Error {
        Self::Encoding(err.to_string())
    }
}

#[derive(Debug)]
pub struct ResponseBuilder {
    req_id: RequestId,
}

impl ResponseBuilder {
    pub fn new(req_id: RequestId) -> Self {
        Self { req_id }
    }

    pub fn completed(self, addresses: SerializablePeerTable) -> DiscoveryResponse {
        self.send(ResponseStatusCode::RequestCompletedFull, Some(addresses))
    }

    pub fn not_found(self) -> DiscoveryResponse {
        self.send(ResponseStatusCode::RequestFailedPeersNotFound, None)
    }
    pub fn rejected(self) -> DiscoveryResponse {
        self.send(ResponseStatusCode::RequestRejected, None)
    }
    fn send(
        self,
        status: ResponseStatusCode,
        addresses: Option<SerializablePeerTable>,
    ) -> DiscoveryResponse {
        let msg = DiscoveryResponse {
            id: self.req_id,
            status,
            addresses,
        };

        return msg;
    }
}
