mod store;
mod stream;
mod thread_pool;
pub mod transport;

use blockstore::memory::MemoryDB as BlockstoreMemory;
use blockstore::types::BlockStore;
use data_transfer::{Dt, DtOptions, DtParams};
use futures::channel::{mpsc, oneshot};
use futures::StreamExt;
use graphsync::traversal::unixfs_path_selector;
use js_sys::{Promise, Uint8Array};
use libp2p::{
    core,
    core::muxing::StreamMuxerBox,
    core::transport::{Boxed, OptionalTransport, Transport},
    identity, mplex, noise, Multiaddr, PeerId,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use store::CacheStore;
use stream::{IntoUnderlyingSource, QueuingStrategy, ReadableStream};
use thread_pool::run_bg;
use wasm_bindgen::JsCast;
use web_sys::{
    DedicatedWorkerGlobalScope, MessageEvent, ReadableStream as JsStream, Response, ResponseInit,
};

use wasm_bindgen::prelude::*;

// pub use console_error_panic_hook::set_once as set_console_error_panic_hook;
pub use console_log::init_with_level as init_console_log;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestParams {
    pub maddress: String,
    pub peer_id: String,
    pub cid: String,
}

#[derive(Serialize, Deserialize)]
pub struct ResponseHeaders {
    #[serde(rename = "Content-Type")]
    pub content_type: String,
}

#[wasm_bindgen]
pub struct Node {
    client: Dt<CacheStore>,
}

// encapsulates a common transport and peer identity for each request.
// will throw an error if initialized twice.
#[wasm_bindgen]
impl Node {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        console_error_panic_hook::set_once();
        init_console_log(log::Level::from_str("info").unwrap()).unwrap();

        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        log::info!("Local peer id: {:?}", local_peer_id);

        let store = Arc::new(CacheStore::default());
        let transport = build_transport(local_key.clone());

        let client = Dt::new(
            local_peer_id,
            store,
            transport,
            DtOptions::default().with_executor(Box::new(move |fut| {
                run_bg(fut).unwrap();
            })),
        );
        Self { client }
    }
    pub fn spawn_request(&self, js_params: JsValue) -> Result<Promise, JsValue> {
        let client = self.client.clone();

        // JsValue is not safe to share between thread so we must deserialize on main thread
        let params: RequestParams = js_params.into_serde().map_err(js_err)?;

        let maddr: Multiaddr = params.maddress.parse().map_err(js_err)?;
        let peer_id = PeerId::from_str(&params.peer_id).map_err(js_err)?;

        let params = DtParams::new(params.cid).map_err(|e| js_err(e))?;

        let done = async move {
            match client.pull(peer_id, maddr, params).await {
                Ok(stream) => {
                    let ct = stream.content_type;
                    // the mpsc receiver is mapped into a readable stream source.
                    let source =
                        IntoUnderlyingSource::new(Box::new(stream.channel.map(
                            |item| match item {
                                Ok(bytes) => {
                                    let data: JsValue =
                                        js_sys::Uint8Array::from(bytes.as_slice()).into();
                                    Ok(data)
                                }
                                Err(e) => Err(js_err(e)),
                            },
                        )));
                    // Set HWM to 0 to prevent the JS ReadableStream from buffering chunks in its queue,
                    // since the original Rust stream is better suited to handle that.
                    let strategy = QueuingStrategy::new(0.0);

                    let js_stream: JsStream =
                        ReadableStream::new_with_source(source, strategy).unchecked_into();

                    let headers = JsValue::from_serde(&ResponseHeaders {
                        content_type: ct.into(),
                    })
                    .map_err(js_err)?;

                    let mut init = ResponseInit::new();

                    let res = Response::new_with_opt_readable_stream_and_init(
                        Some(&js_stream),
                        init.headers(&headers).status(200),
                    )?;
                    Ok(res.into())
                }
                Err(e) => Err(js_err(e)),
            }
        };
        Ok(wasm_bindgen_futures::future_to_promise(done))
    }
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
