use async_std::task;
use blockstore::types::BlockStore;
use dag_service;
use data_transfer::{DataTransfer, DealParams};
use graphsync::traversal::{RecursionLimit, Selector};
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use libp2p::{
    core,
    core::muxing::StreamMuxerBox,
    core::transport::{Boxed, OptionalTransport},
    dns, identity, mplex, noise, tcp, websocket, Multiaddr, PeerId, Swarm, Transport,
};
use std::collections::HashMap;
use std::fs::File;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use warp::{http, Filter};

pub async fn start_server<B: 'static + BlockStore>(blockstore: Arc<B>, behaviour: DataTransfer<B>)
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
}

pub async fn read_file<B: BlockStore>(
    path: String,
    store: Arc<B>,
) -> Result<impl warp::Reply, warp::Rejection>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    match File::open(path) {
        Ok(mut f) => {
            let res = dag_service::add_from_read(store.clone(), &mut f);
            match res {
                Ok(root) => {
                    let resp = format!("added file {:?} to blockstore", root.unwrap().to_string());
                    println!("{:?}", resp);
                    Ok(warp::reply::with_status(resp, http::StatusCode::CREATED))
                }
                Err(e) => {
                    let resp = format!("failed to write to buffer: {}", e.to_string());
                    println!("{:?}", resp);
                    Ok(warp::reply::with_status(
                        resp,
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                    ))
                }
            }
        }
        Err(e) => {
            let resp = format!("invalid path to file: {}", e.to_string());
            println!("{:?}", resp);
            Ok(warp::reply::with_status(
                resp,
                http::StatusCode::BAD_REQUEST,
            ))
        }
    }
}

pub async fn export_file<B: BlockStore>(
    key: String,
    path: String,
    store: Arc<B>,
) -> Result<impl warp::Reply, warp::Rejection>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    match Cid::try_from(key) {
        Ok(cid) => {
            let file = File::create(path).unwrap();
            let res = dag_service::cat_to_write(store.clone(), cid, file);
            match res {
                Ok(_) => {
                    let resp = format!("loaded file {:?} from blockstore", cid.to_string());
                    println!("{:?}", resp);
                    Ok(warp::reply::with_status(resp, http::StatusCode::CREATED))
                }
                Err(e) => {
                    let resp = format!("failed to write to file: {}", e.to_string());
                    println!("{:?}", resp);
                    Ok(warp::reply::with_status(
                        resp,
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                    ))
                }
            }
        }
        Err(e) => {
            let resp = format!("invalid cid: {}", e.to_string());
            println!("{:?}", resp);
            Ok(warp::reply::with_status(
                resp,
                http::StatusCode::BAD_REQUEST,
            ))
        }
    }
}

pub async fn retrieve_file<B: 'static + BlockStore>(
    key: String,
    peer: String,
    multiaddr: String,
    swarm: Arc<Mutex<Swarm<DataTransfer<B>>>>,
) -> Result<impl warp::Reply, warp::Rejection>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    log::info!("here");
    let selector = Selector::ExploreRecursive {
        limit: RecursionLimit::None,
        sequence: Box::new(Selector::ExploreAll {
            next: Box::new(Selector::ExploreRecursiveEdge),
        }),
        current: None,
    };

    match Cid::try_from(key) {
        Ok(cid) => match PeerId::from_str(&peer) {
            Ok(peer) => match Multiaddr::try_from(multiaddr) {
                Ok(multiaddr) => {
                    // make the peer dialable
                    log::info!("loaded all params");
                    swarm
                        .lock()
                        .unwrap()
                        .behaviour_mut()
                        .add_address(&peer, multiaddr);
                    log::info!("loaded all params");
                    match swarm.lock().unwrap().behaviour_mut().pull(
                        peer,
                        cid,
                        selector,
                        DealParams::default(),
                    ) {
                        Ok(_) => {
                            let resp = format!("transfer started in background");
                            Ok(warp::reply::with_status(resp, http::StatusCode::OK))
                        }
                        Err(e) => {
                            let resp = format!("transfer failed: {}", e.to_string());
                            println!("{:?}", resp);
                            Ok(warp::reply::with_status(
                                resp,
                                http::StatusCode::INTERNAL_SERVER_ERROR,
                            ))
                        }
                    }
                }
                Err(e) => {
                    let resp = format!("invalid multiaddress: {:?}", e);
                    println!("{:?}", resp);
                    Ok(warp::reply::with_status(
                        resp,
                        http::StatusCode::BAD_REQUEST,
                    ))
                }
            },

            Err(e) => {
                let resp = format!("invalid peer id: {:?}", e);
                println!("{:?}", resp);
                Ok(warp::reply::with_status(
                    resp,
                    http::StatusCode::BAD_REQUEST,
                ))
            }
        },
        Err(e) => {
            let resp = format!("invalid cid: {}", e.to_string());
            println!("{:?}", resp);
            Ok(warp::reply::with_status(
                resp,
                http::StatusCode::BAD_REQUEST,
            ))
        }
    }
}

/// Builds the transport stack that LibP2P will communicate over.
pub fn build_transport(local_key: identity::Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let transport = tcp::TcpConfig::new().nodelay(true);
    let transport = websocket::WsConfig::new(transport.clone()).or_transport(transport);
    let transport = OptionalTransport::some(
        if let Ok(dns) = task::block_on(dns::DnsConfig::system(transport.clone())) {
            dns.boxed()
        } else {
            transport.clone().map_err(dns::DnsErr::Transport).boxed()
        },
    )
    .or_transport(transport);

    let auth_config = {
        let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&local_key)
            .expect("Noise key generation failed");

        noise::NoiseConfig::xx(dh_keys).into_authenticated()
    };

    let mut mplex_config = mplex::MplexConfig::new();
    mplex_config.set_max_buffer_size(usize::MAX);

    transport
        .upgrade(core::upgrade::Version::V1)
        .authenticate(auth_config)
        .multiplex(mplex_config)
        .timeout(Duration::from_secs(20))
        .boxed()
}
