importScripts("pop.js");

self.onmessage = (event) => {
  // Load the wasm file by awaiting the Promise returned by `wasm_bindgen`.
  let initialized = wasm_bindgen(...event.data).catch((err) => {
    setTimeout(() => {
      throw err;
    });
    throw err;
  });

  self.onmessage = async (event) => {
    await initialized;
    wasm_bindgen.request(event.data);
  };
};
