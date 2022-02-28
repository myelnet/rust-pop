import * as React from "react";
import { useState, useEffect } from "react";
import * as ReactDOM from "react-dom";

// if ("serviceWorker" in navigator) {
//   // window.addEventListener("load", function() {
//   navigator.serviceWorker.register("sw.js").then(
//     function (registration) {
//       // Registration was successful
//       console.log(
//         "ServiceWorker registration successful with scope: ",
//         registration.scope
//       );
//     },
//     function (err) {
//       // registration failed :(
//       console.log("ServiceWorker registration failed: ", err);
//     }
//   );
//   // });
// }

function App() {
  const [root, setRoot] = useState("");
  const [maddr, setMaddr] = useState("");

  function sendRequest() {
    if (!root || !maddr) {
      return;
    }
    wasm_bindgen("pop_bg.wasm")
      .then(() => {
        console.log("wasm loaded");
        const start = performance.now();
        //@ts-ignore
        const { request_bg } = wasm_bindgen;
        const parts = maddr.split("/p2p/");
        request_bg({
          logLevel: "info",
          maddress: parts[0],
          peerId: parts[1],
          cid: root,
        })
          .blob()
          .then((blob) => {
            const done = performance.now();
            const duration = done - start;
            console.log(`done in ${duration}ms (${blob.size / duration}bps)`);
          })
          .catch(console.error);
      })
      .catch(console.error);
  }
  useEffect(() => {
    // @ts-ignore
    // if (wasm_bindgen) {
    //   console.log("starting wasm");
    // @ts-ignore
    // wasm_bindgen("pop_bg.wasm")
    // .then(() => {
    //@ts-ignore
    // const { DagService, WorkerPool } = wasm_bindgen;
    // const pool = new WorkerPool(1);
    // const dag = new DagService();
    // dag
    //   .string_to_block("hellot world", pool)
    //   .then(console.log)
    //   .catch(console.error);
    // })
    // .catch(console.error);
    // }
  }, []);
  return (
    <div className="app">
      <input
        id="root"
        type="text"
        autoComplete="off"
        spellCheck="false"
        placeholder="root CID"
        className="ipt"
        value={root}
        onChange={(e) => setRoot(e.target.value)}
      />
      <input
        id="maddr"
        type="text"
        autoComplete="off"
        spellCheck="false"
        placeholder="multi address"
        className="ipt"
        value={maddr}
        onChange={(e) => setMaddr(e.target.value)}
      />
      <button className="btn" onClick={sendRequest}>
        request
      </button>
    </div>
  );
}

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById("root")
);
