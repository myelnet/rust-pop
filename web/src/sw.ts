import init, { request } from "./wasm/pop.js";

export type {};
declare let self: ServiceWorkerGlobalScope;

export function toPathComponents(path = ""): string[] {
  // split on / unless escaped with \
  return (path.trim().match(/([^\\^/]|\\\/)+/g) || []).filter(Boolean);
}

class Controller {
  private _installAndActiveListenersAdded?: boolean;

  constructor() {
    this.install = this.install.bind(this);
    this.activate = this.activate.bind(this);
  }

  start(): void {
    if (!this._installAndActiveListenersAdded) {
      self.addEventListener("install", this.install);
      self.addEventListener("activate", this.activate);
      self.addEventListener("fetch", ((event: FetchEvent) => {
        const url = new URL(event.request.url);
        console.log(url);

        const sparams = url.searchParams;
        const peerAddr = sparams.get("peer");

        const info = peerAddr?.split("/p2p/")!;

        const comps = toPathComponents(url.pathname);

        const params = {
          logLevel: "info",
          maddress: info[0],
          peerId: info[1],
          cid: comps[0],
        };

        event.respondWith(
          request(params)
            .then(() => new Response("success"))
            .catch((err) => {
              return new Response(err);
            })
        );
      }) as EventListener);
      this._installAndActiveListenersAdded = true;
    }
  }

  install(event: ExtendableEvent): Promise<void> {
    const promise = (async () => {
      await init("pop_bg.wasm");
      return self.skipWaiting();
    })();
    event.waitUntil(promise);
    return promise;
  }

  activate(event: ExtendableEvent): Promise<void> {
    const promise = (async () => {
      // TODO: cleanup any content we don't need anymore
      return self.clients.claim();
    })();
    event.waitUntil(promise);
    return promise;
  }
}

const ctrl = new Controller();
ctrl.start();
