use crate::native::server::{
    build_transport, export_file, handle_rejection, import_car, node_info, read_file, resolve_file,
    retrieve_file,
};
use blockstore::types::BlockStore;
use data_transfer::{Dt, DtOptions};
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::Ipld;
use libp2p::{self, identity, Multiaddr, PeerId};
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use wallet::{KeyInfo, KeyStore, KeyStoreConfig, KeyType};
use warp::Filter;

pub struct Node<B: 'static + BlockStore + Clone>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    pub dt: Dt<B>,
    pub store: Arc<B>,
}

pub struct NodeConfig<B: 'static + BlockStore + Clone> {
    pub listening_multiaddr: Option<Multiaddr>,
    pub blockstore: B,
    pub peer_key: identity::Keypair,
}

impl<B: BlockStore + Clone> Node<B>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    pub fn new(config: NodeConfig<B>) -> Self {
        //  unwrap because we want to panic if there was an issue
        let local_key = config.peer_key;

        let local_peer_id = PeerId::from(local_key.public());

        let transport = build_transport(local_key.clone());

        let store = Arc::new(config.blockstore);

        let mut options = DtOptions::default();
        if let Some(maddr) = config.listening_multiaddr {
            options = options.as_listener(maddr);
        }

        Node {
            dt: Dt::new(local_peer_id, store.clone(), transport, options),
            store,
        }
    }

    pub async fn start_server(self) {
        let dt = self.dt.clone();
        let store = self.store.clone();
        let store_filter = warp::any().map(move || store.clone());
        let dt_filter = warp::any().map(move || dt.clone());

        let cors = warp::cors()
            .allow_any_origin()
            .allow_methods(vec!["GET"])
            .allow_headers(vec!["Content-Type", "User-Agent", "Range"]);

        let add_file = warp::post()
            .and(warp::path("add"))
            .and(warp::body::bytes())
            .map(|bytes: warp::hyper::body::Bytes| {
                return std::str::from_utf8(&bytes).unwrap().to_string();
            })
            .and(store_filter.clone())
            .and_then(|path: String, store: Arc<B>| {
                return read_file(path, store);
            });

        let import_car = warp::post()
            .and(warp::path("import"))
            .and(warp::body::bytes())
            .map(|bytes: warp::hyper::body::Bytes| {
                return std::str::from_utf8(&bytes).unwrap().to_string();
            })
            .and(store_filter.clone())
            .and_then(|path: String, store: Arc<B>| {
                return import_car(path, store);
            });

        let export_file = warp::post()
            .and(warp::path("export"))
            .and(warp::body::json())
            .and(store_filter.clone())
            .and_then(|simple_map: HashMap<String, String>, store: Arc<B>| {
                //  can safely unwrap entries as if they are None the method will just return a failure
                //  response to the requesting client
                return export_file(
                    simple_map.get("cid").unwrap().to_string(),
                    simple_map.get("path").unwrap().to_string(),
                    store.clone(),
                );
            });

        let retrieve_file = warp::post()
            .and(warp::path("retrieve"))
            .and(warp::body::json())
            .and(dt_filter.clone())
            .and_then(move |simple_map: HashMap<String, String>, dt: Dt<B>| {
                //  can safely unwrap entries as if they are None the method will just return a failure
                //  response to the requesting client
                return retrieve_file(
                    simple_map.get("cid").unwrap().to_string(),
                    simple_map.get("peer").unwrap().to_string(),
                    simple_map.get("multiaddr").unwrap().to_string(),
                    dt,
                );
            });

        let get_node_info = warp::post()
            .and(warp::path("info"))
            .and(dt_filter.clone())
            .and_then(|dt: Dt<B>| node_info(dt.clone()));

        let get_file = warp::get()
            .and(warp::path::param())
            .and(store_filter.clone())
            .and_then(|path: String, store: Arc<B>| resolve_file(path, store));

        let routes = add_file
            .or(import_car)
            .or(export_file)
            .or(retrieve_file)
            .or(get_node_info)
            .or(get_file)
            .recover(handle_rejection)
            .with(cors);
        // serve on port 2002
        warp::serve(routes).run(([127, 0, 0, 1], 2002)).await
    }
}

pub fn get_peer_key(pop_dir: PathBuf) -> Result<identity::Keypair, Box<dyn Error>> {
    let mut key_store = KeyStore::new(KeyStoreConfig::Persistent(pop_dir))?;
    //  check we have at least one Ed25519 keys in the store
    let peer_infos = key_store.list_info();
    if let Some(key) = peer_infos
        .iter()
        .filter(|info| info.key_type == KeyType::Ed25519)
        .next()
    {
        let local_key = identity::Keypair::from_protobuf_encoding(&key.private_key())?;
        Ok(local_key)
    }
    // if not then generate a new one and insert it into keystore
    else {
        let local_key = identity::Keypair::generate_ed25519();
        // use public peer ID as the key
        let pub_key = local_key.public().to_peer_id().to_string();
        // we'll use protobuf encoding for storage
        let priv_key = local_key.to_protobuf_encoding()?;
        key_store.put(pub_key, KeyInfo::new(KeyType::Ed25519, priv_key))?;
        //  flush to disk
        key_store.flush()?;
        Ok(local_key)
    }
}
