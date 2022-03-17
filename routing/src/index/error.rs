use std::error::Error as StdError;
use thiserror::Error;

/// HAMT Error
#[derive(Debug, Error)]
pub enum Error {
    /// Maximum depth error
    #[error("Maximum depth reached")]
    MaxDepth,
    /// Hash bits does not support greater than 8 bit width
    #[error("HashBits does not support retrieving more than 8 bits")]
    InvalidHashBitLen,
    /// This should be treated as a fatal error, must have at least one pointer in node
    #[error("Invalid HAMT format, node cannot have 0 pointers")]
    ZeroPointers,
    /// Cid not found in store error
    #[error("Cid ({0}) did not match any in database")]
    CidNotFound(String),
    /// Cid was not a HAMT node
    #[error("Cid was not a HAMT node")]
    InvalidNode,
    /// Dynamic error for when the error needs to be forwarded as is.
    #[error("{0}")]
    Dynamic(Box<dyn StdError>),
    /// Custom HAMT error
    #[error("{0}")]
    Other(String),
}
