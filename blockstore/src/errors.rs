use libipld::Cid;
use serde_cbor::error::Error as CborError;
use thiserror::Error;

/// BlockStore error
#[derive(Debug, Error)]
pub enum Error {
    #[error("BlockStore: block not found for {0}")]
    BlockNotFound(Cid),
    #[error("BlockStore: skip missing block {0}")]
    SkipMe(Cid),
    #[error("Invalid bulk write kv lengths, must be equal")]
    InvalidBulkLen,
    #[error("Cannot use unopened database")]
    Unopened,
    #[error(transparent)]
    #[cfg(feature = "native")]
    Database(#[from] rocksdb::Error),
    #[error(transparent)]
    Encoding(#[from] CborError),
    #[error("{0}")]
    Other(String),
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        use Error::*;

        match (self, other) {
            (&BlockNotFound(a), &BlockNotFound(b)) => a == b,
            (&SkipMe(a), &SkipMe(b)) => a == b,
            (&InvalidBulkLen, &InvalidBulkLen) => true,
            (&Unopened, &Unopened) => true,
            #[cfg(feature = "native")]
            (&Database(_), &Database(_)) => true,
            (&Encoding(_), &Encoding(_)) => true,
            (&Other(ref a), &Other(ref b)) => a == b,
            _ => false,
        }
    }
}

impl From<Error> for String {
    fn from(e: Error) -> Self {
        e.to_string()
    }
}
