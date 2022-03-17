use blockstore::{errors::Error, types::BlockStore};
use libipld::DefaultParams;
use libipld::{Block, Cid};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures;
use web_sys::{Cache, DedicatedWorkerGlobalScope, Response};

pub async fn open_cache_store() -> Result<Cache, JsValue> {
    let global = js_sys::global().unchecked_into::<DedicatedWorkerGlobalScope>();
    let caches = global.caches()?;
    let result = wasm_bindgen_futures::JsFuture::from(caches.open("/block")).await?;
    Ok(result.unchecked_into::<Cache>())
}

#[derive(Default, Clone)]
pub struct CacheStore;

impl BlockStore for CacheStore {
    type Params = DefaultParams;

    fn get(&self, cid: &Cid) -> Result<Block<Self::Params>, Error> {
        Err(Error::BlockNotFound(*cid))
    }
    fn insert(&self, block: &Block<Self::Params>) -> Result<(), Error> {
        Ok(())
    }
    fn evict(&self, cid: &Cid) -> Result<(), Error> {
        Ok(())
    }
    fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        Ok(false)
    }
}
