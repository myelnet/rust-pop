import * as React from "react";
import { useState, useEffect } from "react";
import * as ReactDOM from "react-dom";

const CID_KEY = "/cid/default";
const ADDR_KEY = "/maddr/default";

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

class Client {
  node: any;
  pool: any;
  constructor() {
    //@ts-ignore
    const { Node, WorkerPool } = wasm_bindgen;
    this.node = new Node();
    this.pool = new WorkerPool(2);
  }
  fetch(path: string, maddr: string): Promise<Response> {
    const parts = maddr.split("/p2p/");
    return this.node.spawn_request(
      {
        maddress: parts[0],
        peerId: parts[1],
        cid: path,
      },
      this.pool
    );
  }
}

function App() {
  const [root, setRoot] = useState(localStorage.getItem(CID_KEY) ?? "");
  const [maddr, setMaddr] = useState(localStorage.getItem(ADDR_KEY) ?? "");
  const [img, setImg] = useState("");
  const [vid, setVid] = useState("");
  const [loading, setLoading] = useState(false);
  const [client, setClient] = useState<Client | null>(null);

  const disabled = !root || !maddr || loading;

  function sendRequest() {
    if (disabled || !client) {
      return;
    }
    setLoading(true);
    const start = performance.now();
    client
      .fetch(root, maddr)
      .then((res) => res.blob())
      .then((blob) => {
        const url = URL.createObjectURL(blob);
        setLoading(false);
        if (/image/.test(blob.type)) {
          setImg(url);
        }
        if (/video/.test(blob.type)) {
          setVid(url);
        }
        const done = performance.now();
        const duration = done - start;
        console.log(`done in ${duration}ms (${blob.size / duration}bps)`);

        localStorage.setItem(CID_KEY, root);
        localStorage.setItem(ADDR_KEY, maddr);
      })
      .catch(console.error);
  }
  useEffect(() => {
    // @ts-ignore
    if (wasm_bindgen) {
      // @ts-ignore
      wasm_bindgen("pop_bg.wasm").then(() => {
        setClient(new Client());
      });
    }
  }, []);
  return (
    <div className="app">
      {img ? (
        <img className="img" src={img} alt="Retrieved image" />
      ) : vid ? (
        <video controls className="img" autoPlay loop>
          <source src={vid} type="video/mp4" />
        </video>
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
      <button className="btn" onClick={sendRequest} disabled={disabled}>
        request
      </button>
      <p className="p">{!!client && "wasm loaded"}</p>
    </div>
  );
}

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById("root")
);
