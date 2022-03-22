//! Bitswap protocol implementation
#![deny(missing_docs)]
#![deny(warnings)]

mod behaviour;
#[cfg(feature = "compat")]
mod compat;
mod protocol;
mod query;
mod stats;

pub use crate::behaviour::{Bitswap, BitswapConfig, BitswapEvent, Channel};
pub use crate::query::QueryId;
