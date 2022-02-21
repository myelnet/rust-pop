importScripts("pop.js");

self.onmessage = (event) => {
  // Load the wasm file by awaiting the Promise returned by `wasm_bindgen`.
  let initialized = wasm_bindgen("pop_bg.wasm").catch((err) => {
    setTimeout(() => {
      throw err;
    });
    throw err;
  });

  console.log("worker received message");

  initialized.then(() => {
    wasm_bindgen
      .request(event.data)
      .then(() => self.postMessage(undefined))
      .catch((err) => console.log(err));
  });
};
