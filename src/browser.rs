use std::str::FromStr;

use blockstore::memory::MemoryDB as BlockstoreMemory;

use wasm_bindgen::prelude::*;

use libp2p_wasm_ext::{ffi, ExtTransport};

// pub use console_error_panic_hook::set_once as set_console_error_panic_hook;
pub use console_log::init_with_level as init_console_log;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    alert("hello from rust-wasm");
}

#[wasm_bindgen]
pub struct Client {}

/// Starts the client.
#[wasm_bindgen]
pub async fn start_client(log_level: String) -> Result<Client, JsValue> {
    start_inner(log_level)
        .await
        .map_err(|err| JsValue::from_str(&err.to_string()))
}

async fn start_inner(
    log_level: String,
) -> Result<Client, Box<dyn std::error::Error>> {
    console_error_panic_hook::set_once();
    init_console_log(log::Level::from_str(&log_level).unwrap()).unwrap();

    let transport = ExtTransport::new(ffi::websocket_transport());

    let config = crate::NodeConfig {
        listening_multiaddr: "".parse()?,
        wasm_external_transport: Some(transport),
        blockstore: BlockstoreMemory::default()
    };
    wasm_bindgen_futures::spawn_local(crate::Node::new(config).run());

    Ok(Client {})
}
