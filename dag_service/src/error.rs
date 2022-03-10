use libipld::error::Error as IpldError;
use std::io::{Error as IoError, ErrorKind};
use thiserror::Error;

#[derive(Debug, PartialEq, Error)]
pub enum Error {
    #[error("Parsing error: {0}")]
    Parsing(String),
    #[error("Invalid file: {0}")]
    InvalidFile(String),
    #[error("{0}")]
    Other(&'static str),
    #[error("BlockStore: {0}")]
    Store(String),
    #[error("Eof")]
    Eof,
    #[error("io: {0}")]
    Io(String),
    #[error("ipld: {0}")]
    Ipld(String),
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Error {
        match err.kind() {
            ErrorKind::UnexpectedEof => Error::Eof,
            _ => Error::Io(err.to_string()),
        }
    }
}

impl From<IpldError> for Error {
    fn from(err: IpldError) -> Error {
        Error::Ipld(err.to_string())
    }
}
