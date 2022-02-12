import * as React from "react";
import { useState } from "react";
import * as ReactDOM from "react-dom";

if ("serviceWorker" in navigator) {
  // window.addEventListener("load", function() {
  navigator.serviceWorker.register("sw.js").then(
    function (registration) {
      // Registration was successful
      console.log(
        "ServiceWorker registration successful with scope: ",
        registration.scope
      );
    },
    function (err) {
      // registration failed :(
      console.log("ServiceWorker registration failed: ", err);
    }
  );
  // });
}

function App() {
  const [root, setRoot] = useState("");
  const [maddr, setMaddr] = useState("");
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
      <button className="btn">request</button>
    </div>
  );
}

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById("root")
);
