use std::error::Error;

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = pop::NodeConfig {
        listening_multiaddr: "/ip4/0.0.0.0/tcp/0".parse()?,
        wasm_external_transport: None,
    };
    let node = pop::Node::new(config);
    node.run().await;

    Ok(())
}
