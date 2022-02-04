mod empty_map;
mod graphsync;
mod graphsync_pb;
mod network;
mod node;
mod request_manager;
mod response_manager;
mod traversal;

use node::{Node, NodeConfig};
use std::error::Error;

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = NodeConfig {
        listening_multiaddr: "/ip4/0.0.0.0/tcp/0".parse()?,
    };
    let node = Node::new(config);
    node.run().await;

    Ok(())
}
