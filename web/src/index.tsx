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
//
function Spinner() {
  return (
    <div className="spin" role="progressbar">
      <svg height="100%" viewBox="0 0 32 32" width="100%">
        <circle
          cx="16"
          cy="16"
          fill="none"
          r="14"
          strokeWidth="4"
          style={{
            stroke: "#000",
            opacity: 0.2,
          }}
        />
        <circle
          cx="16"
          cy="16"
          fill="none"
          r="14"
          strokeWidth="4"
          style={{
            stroke: "#000",
            strokeDasharray: 80,
            strokeDashoffset: 60,
          }}
        />
      </svg>
    </div>
  );
}

function App() {
  const [root, setRoot] = useState("");
  const [maddr, setMaddr] = useState("");
  const [img, setImg] = useState("");
  const [loading, setLoading] = useState(false);

  function sendRequest() {
    if (!root || !maddr) {
      return;
    }
    setLoading(true);
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
          .then((res) => res.blob())
          .then((blob) => {
            const url = URL.createObjectURL(blob);
            setLoading(false);
            setImg(url);
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
      {img ? (
        <img className="img" src={img} alt="Retrieved image" />
      ) : (
        <div className="img">{loading && <Spinner />}</div>
      )}
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
      <button className="btn" onClick={sendRequest} disabled={loading}>
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
