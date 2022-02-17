use serde_cbor::error::Error as CborError;
use std::io::Error as StdError;
use thiserror::Error;

#[derive(Debug, PartialEq, Error)]
pub enum Error {
    #[error("{0}")]
    Encoding(String),
    #[error("{0}")]
    Other(&'static str),
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
