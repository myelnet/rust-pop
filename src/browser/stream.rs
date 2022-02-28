use futures::future::{abortable, AbortHandle, TryFutureExt};
use futures::stream::{Stream, TryStreamExt};
use js_sys::Promise;
use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

#[wasm_bindgen]
extern "C" {
    /// A raw [`ReadableStream`](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream).
    ///
    /// This represents the same JavaScript objects as [`web_sys::ReadableStream`].
    /// If you're using an API that returns such an object, you can cast it to this type using
    /// [`unchecked_into`][wasm_bindgen::JsCast::unchecked_into].
    #[wasm_bindgen(js_name = ReadableStream, typescript_type = "ReadableStream")]
    #[derive(Clone, Debug)]
    pub type ReadableStream;

    #[wasm_bindgen(constructor)]
    pub(crate) fn new_with_source(
        source: IntoUnderlyingSource,
        strategy: QueuingStrategy,
    ) -> ReadableStream;
}

#[wasm_bindgen]
extern "C" {
    /// A raw [`ReadableStreamDefaultController`](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStreamDefaultController).
    #[derive(Clone, Debug)]
    pub type ReadableStreamDefaultController;

    #[wasm_bindgen(method, getter, js_name = desiredSize)]
    pub fn desired_size(this: &ReadableStreamDefaultController) -> Option<f64>;

    #[wasm_bindgen(method, js_name = close)]
    pub fn close(this: &ReadableStreamDefaultController);

    #[wasm_bindgen(method, js_name = enqueue)]
    pub fn enqueue(this: &ReadableStreamDefaultController, chunk: &JsValue);

    #[wasm_bindgen(method, js_name = error)]
    pub fn error(this: &ReadableStreamDefaultController, error: &JsValue);
}

type JsValueStream = dyn Stream<Item = Result<JsValue, JsValue>>;

#[wasm_bindgen]
pub(crate) struct IntoUnderlyingSource {
    inner: Rc<RefCell<Inner>>,
    pull_handle: Option<AbortHandle>,
}

impl IntoUnderlyingSource {
    pub fn new(stream: Box<JsValueStream>) -> Self {
        IntoUnderlyingSource {
            inner: Rc::new(RefCell::new(Inner::new(stream))),
            pull_handle: None,
        }
    }
}

#[wasm_bindgen]
impl IntoUnderlyingSource {
    pub fn pull(&mut self, controller: ReadableStreamDefaultController) -> Promise {
        let inner = self.inner.clone();
        let fut = async move {
            // This mutable borrow can never panic, since the ReadableStream always queues
            // each operation on the underlying source.
            let mut inner = inner.try_borrow_mut().unwrap_throw();
            inner.pull(controller).await
        };

        // Allow aborting the future from cancel().
        let (fut, handle) = abortable(fut);
        // Ignore errors from aborting the future.
        let fut = fut.unwrap_or_else(|_| Ok(JsValue::undefined()));

        self.pull_handle = Some(handle);
        future_to_promise(fut)
    }

    pub fn cancel(self) {
        // The stream has been canceled, drop everything.
        drop(self);
    }
}

impl Drop for IntoUnderlyingSource {
    fn drop(&mut self) {
        // Abort the pending pull, if any.
        if let Some(handle) = self.pull_handle.take() {
            handle.abort();
        }
    }
}

struct Inner {
    stream: Option<Pin<Box<JsValueStream>>>,
}

impl Inner {
    fn new(stream: Box<JsValueStream>) -> Self {
        Inner {
            stream: Some(stream.into()),
        }
    }

    async fn pull(
        &mut self,
        controller: ReadableStreamDefaultController,
    ) -> Result<JsValue, JsValue> {
        // The stream should still exist, since pull() will not be called again
        // after the stream has closed or encountered an error.
        let stream = self.stream.as_mut().unwrap_throw();
        match stream.try_next().await {
            Ok(Some(chunk)) => controller.enqueue(&chunk),
            Ok(None) => {
                // The stream has closed, drop it.
                self.stream = None;
                controller.close();
            }
            Err(err) => {
                // The stream encountered an error, drop it.
                self.stream = None;
                return Err(err);
            }
        };
        Ok(JsValue::undefined())
    }
}

#[wasm_bindgen]
#[derive(Debug)]
pub(crate) struct QueuingStrategy {
    high_water_mark: f64,
}

impl QueuingStrategy {
    pub fn new(high_water_mark: f64) -> Self {
        Self { high_water_mark }
    }
}

#[wasm_bindgen]
impl QueuingStrategy {
    #[wasm_bindgen(getter, js_name = highWaterMark)]
    pub fn high_water_mark(&self) -> f64 {
        self.high_water_mark
    }
}
