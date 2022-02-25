pub mod node;

pub use crate::browser::node::{Node, NodeConfig};
use blockstore::memory::MemoryDB as BlockstoreMemory;
use libipld::Cid;
use libp2p::{
    core,
    core::muxing::StreamMuxerBox,
    core::transport::{Boxed, OptionalTransport, Transport},
    identity, mplex, noise, Multiaddr, PeerId,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::string::ToString;
use std::time::Duration;

use wasm_bindgen::prelude::*;

// pub use console_error_panic_hook::set_once as set_console_error_panic_hook;
pub use console_log::init_with_level as init_console_log;

pub mod transport;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestParams {
    pub log_level: String,
    pub maddress: String,
    pub peer_id: String,
    pub cid: String,
}

#[wasm_bindgen]
pub async fn request(js_params: JsValue) -> Result<(), JsValue> {
    let params: RequestParams = js_params.into_serde().map_err(js_err)?;

    console_error_panic_hook::set_once();
    init_console_log(log::Level::from_str(&params.log_level).unwrap()).unwrap();

    let maddr: Multiaddr = params.maddress.parse().map_err(js_err)?;
    let peer_id = PeerId::from_str(&params.peer_id).map_err(js_err)?;
    let cid = Cid::try_from(params.cid).map_err(js_err)?;

    let config = NodeConfig {
        listening_multiaddr: None,
        blockstore: BlockstoreMemory::default(),
    };

    let mut node = Node::new(config);

    node.run_request(maddr, peer_id, cid).await;

    Ok(())
}

fn js_err<E: ToString + Send + Sync + 'static>(e: E) -> JsValue {
    JsValue::from_str(&e.to_string())
}

pub fn build_transport(local_key: identity::Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let transport = OptionalTransport::some(transport::WsTransport);

    let auth_config = {
        let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&local_key)
            .expect("Noise key generation failed");

        noise::NoiseConfig::xx(dh_keys).into_authenticated()
    };

    let mut mplex_config = mplex::MplexConfig::new();
    mplex_config.set_max_buffer_size(usize::MAX);

    transport
        .upgrade(core::upgrade::Version::V1)
        .authenticate(auth_config)
        .multiplex(mplex_config)
        .timeout(Duration::from_secs(20))
        .boxed()
}
