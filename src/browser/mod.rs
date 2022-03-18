mod store;
mod stream;
pub mod transport;

use blockstore::memory::MemoryDB as BlockstoreMemory;
use blockstore::types::BlockStore;
use data_transfer::{
    client::{Client, ClientOptions},
    PullParams,
};
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
use std::cell::RefCell;
use std::rc::Rc;
use std::str::FromStr;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use store::CacheStore;
use stream::{IntoUnderlyingSource, QueuingStrategy, ReadableStream};
use wasm_bindgen::JsCast;
use web_sys::{
    DedicatedWorkerGlobalScope, MessageEvent, ReadableStream as JsStream, Response, ResponseInit,
};
use web_sys::{ErrorEvent, Event, File, Worker};

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
    client: Client<CacheStore>,
    pool: Rc<WorkerPool>,
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

        let pool = Rc::new(WorkerPool::new(8).unwrap());

        let store = Arc::new(CacheStore::default());
        let transport = build_transport(local_key.clone());
        let epool = pool.clone();
        let client = Client::new(
            local_peer_id,
            store,
            transport,
            ClientOptions::default().with_executor(Box::new(move |fut| {
                epool
                    .run(|| wasm_bindgen_futures::spawn_local(fut))
                    .unwrap();
            })),
        );
        Self { client, pool }
    }
    pub fn spawn_request(&self, js_params: JsValue) -> Result<Promise, JsValue> {
        let client = self.client.clone();

        // JsValue is not safe to share between thread so we must deserialize on main thread
        let params: RequestParams = js_params.into_serde().map_err(js_err)?;

        let maddr: Multiaddr = params.maddress.parse().map_err(js_err)?;
        let peer_id = PeerId::from_str(&params.peer_id).map_err(js_err)?;

        let (cid, selector) = match unixfs_path_selector(params.cid) {
            Some((cid, sel)) => (cid, sel),
            None => return Err(js_err("invalid IPFS path")),
        };

        let params = PullParams {
            selector: Some(selector.clone()),
            ..Default::default()
        };

        let done = async move {
            match client.pull(peer_id, maddr, cid, selector, params).await {
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

#[wasm_bindgen]
pub struct WorkerPool {
    state: Rc<PoolState>,
}

struct PoolState {
    workers: RefCell<Vec<Worker>>,
    callback: Closure<dyn FnMut(Event)>,
}

struct Work {
    func: Box<dyn FnOnce() + Send>,
}

#[wasm_bindgen]
impl WorkerPool {
    /// Creates a new `WorkerPool` which immediately creates `initial` workers.
    ///
    /// The pool created here can be used over a long period of time, and it
    /// will be initially primed with `initial` workers. Currently workers are
    /// never released or gc'd until the whole pool is destroyed.
    ///
    /// # Errors
    ///
    /// Returns any error that may happen while a JS web worker is created and a
    /// message is sent to it.
    #[wasm_bindgen(constructor)]
    pub fn new(initial: usize) -> Result<WorkerPool, JsValue> {
        let pool = WorkerPool {
            state: Rc::new(PoolState {
                workers: RefCell::new(Vec::with_capacity(initial)),
                callback: Closure::wrap(Box::new(|event: Event| {
                    log::info!("unhandled event: {}", event.type_());
                }) as Box<dyn FnMut(Event)>),
            }),
        };
        for _ in 0..initial {
            let worker = pool.spawn()?;
            pool.state.push(worker);
        }

        Ok(pool)
    }

    /// Unconditionally spawns a new worker
    ///
    /// The worker isn't registered with this `WorkerPool` but is capable of
    /// executing work for this wasm module.
    ///
    /// # Errors
    ///
    /// Returns any error that may happen while a JS web worker is created and a
    /// message is sent to it.
    fn spawn(&self) -> Result<Worker, JsValue> {
        log::info!("spawning new worker");
        // TODO: what do do about `./worker.js`:
        //
        // * the path is only known by the bundler. How can we, as a
        //   library, know what's going on?
        // * How do we not fetch a script N times? It internally then
        //   causes another script to get fetched N times...
        let worker = Worker::new("./worker.js")?;

        // With a worker spun up send it the module/memory so it can start
        // instantiating the wasm module. Later it might receive further
        // messages about code to run on the wasm module.
        let array = js_sys::Array::new();
        array.push(&wasm_bindgen::module());
        array.push(&wasm_bindgen::memory());
        worker.post_message(&array)?;

        Ok(worker)
    }

    /// Fetches a worker from this pool, spawning one if necessary.
    ///
    /// This will attempt to pull an already-spawned web worker from our cache
    /// if one is available, otherwise it will spawn a new worker and return the
    /// newly spawned worker.
    ///
    /// # Errors
    ///
    /// Returns any error that may happen while a JS web worker is created and a
    /// message is sent to it.
    fn worker(&self) -> Result<Worker, JsValue> {
        match self.state.workers.borrow_mut().pop() {
            Some(worker) => Ok(worker),
            None => self.spawn(),
        }
    }

    /// Executes the work `f` in a web worker, spawning a web worker if
    /// necessary.
    ///
    /// This will acquire a web worker and then send the closure `f` to the
    /// worker to execute. The worker won't be usable for anything else while
    /// `f` is executing, and no callbacks are registered for when the worker
    /// finishes.
    ///
    /// # Errors
    ///
    /// Returns any error that may happen while a JS web worker is created and a
    /// message is sent to it.
    fn execute(&self, f: impl FnOnce() + Send + 'static) -> Result<Worker, JsValue> {
        let worker = self.worker()?;
        let work = Box::new(Work { func: Box::new(f) });
        let ptr = Box::into_raw(work);
        match worker.post_message(&JsValue::from(ptr as u32)) {
            Ok(()) => Ok(worker),
            Err(e) => {
                unsafe {
                    drop(Box::from_raw(ptr));
                }
                Err(e)
            }
        }
    }

    /// Configures an `onmessage` callback for the `worker` specified for the
    /// web worker to be reclaimed and re-inserted into this pool when a message
    /// is received.
    ///
    /// Currently this `WorkerPool` abstraction is intended to execute one-off
    /// style work where the work itself doesn't send any notifications and
    /// whatn it's done the worker is ready to execute more work. This method is
    /// used for all spawned workers to ensure that when the work is finished
    /// the worker is reclaimed back into this pool.
    fn reclaim_on_message(&self, worker: Worker) {
        let state = Rc::downgrade(&self.state);
        let worker2 = worker.clone();
        let reclaim_slot = Rc::new(RefCell::new(None));
        let slot2 = reclaim_slot.clone();
        let reclaim = Closure::wrap(Box::new(move |event: Event| {
            if let Some(error) = event.dyn_ref::<ErrorEvent>() {
                log::info!("error in worker: {}", error.message());
                // TODO: this probably leaks memory somehow? It's sort of
                // unclear what to do about errors in workers right now.
                return;
            }

            // If this is a completion event then can deallocate our own
            // callback by clearing out `slot2` which contains our own closure.
            if let Some(_msg) = event.dyn_ref::<MessageEvent>() {
                if let Some(state) = state.upgrade() {
                    state.push(worker2.clone());
                }
                *slot2.borrow_mut() = None;
                return;
            }

            log::info!("unhandled event: {}", event.type_());
            // TODO: like above, maybe a memory leak here?
        }) as Box<dyn FnMut(Event)>);
        worker.set_onmessage(Some(reclaim.as_ref().unchecked_ref()));
        *reclaim_slot.borrow_mut() = Some(reclaim);
    }
}

impl WorkerPool {
    /// Executes `f` in a web worker.
    ///
    /// This pool manages a set of web workers to draw from, and `f` will be
    /// spawned quickly into one if the worker is idle. If no idle workers are
    /// available then a new web worker will be spawned.
    ///
    /// Once `f` returns the worker assigned to `f` is automatically reclaimed
    /// by this `WorkerPool`. This method provides no method of learning when
    /// `f` completes, and for that you'll need to use `run_notify`.
    ///
    /// # Errors
    ///
    /// If an error happens while spawning a web worker or sending a message to
    /// a web worker, that error is returned.
    pub fn run(&self, f: impl FnOnce() + Send + 'static) -> Result<(), JsValue> {
        let worker = self.execute(f)?;
        self.reclaim_on_message(worker);
        Ok(())
    }
}

impl PoolState {
    fn push(&self, worker: Worker) {
        worker.set_onmessage(Some(self.callback.as_ref().unchecked_ref()));
        worker.set_onerror(Some(self.callback.as_ref().unchecked_ref()));
        let mut workers = self.workers.borrow_mut();
        for prev in workers.iter() {
            let prev: &JsValue = prev;
            let worker: &JsValue = &worker;
            assert!(prev != worker);
        }
        workers.push(worker);
    }
}

/// Entry point invoked by `worker.js`, a bit of a hack but see the "TODO" above
/// about `worker.js` in general.
#[wasm_bindgen]
pub fn child_entry_point(ptr: u32) -> Result<(), JsValue> {
    let ptr = unsafe { Box::from_raw(ptr as *mut Work) };
    let global = js_sys::global().unchecked_into::<DedicatedWorkerGlobalScope>();
    (ptr.func)();
    global.post_message(&JsValue::undefined())?;
    Ok(())
}
