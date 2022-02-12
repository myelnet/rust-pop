export type {};
declare let self: ServiceWorkerGlobalScope;

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
      }) as EventListener);
      this._installAndActiveListenersAdded = true;
    }
  }

  install(event: ExtendableEvent): Promise<void> {
    const promise = (async () => {
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
