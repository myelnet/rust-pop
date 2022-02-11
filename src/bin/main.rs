use blockstore::db::Db as BlockstoreDB;
use blockstore::lfu::LfuBlockstore;
use libipld::Cid;
use libp2p::{Multiaddr, PeerId};
use pop::{Node, NodeConfig};
use std::error::Error;
use std::str::FromStr;

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = NodeConfig {
        listening_multiaddr: "/ip4/0.0.0.0/tcp/0/ws".parse()?,
        wasm_external_transport: None,
        blockstore: LfuBlockstore::new(0, BlockstoreDB::open("path")?)?,
    };
    let mut node = Node::new(config);

    if let Some(addr) = std::env::args().nth(1) {
        let remote: Multiaddr = addr.parse()?;
        if let Some(peer) = std::env::args().nth(2) {
            let peer_id = PeerId::from_str(&peer)?;
            if let Some(key) = std::env::args().nth(3) {
                let cid = Cid::try_from(key)?;
                node.run_request(remote, peer_id, cid).await;
            }
        }
    } else {
        node.fill_random_data();
    }
    node.run().await;

    Ok(())
}
