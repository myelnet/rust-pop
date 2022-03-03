use async_std::task;
use blockstore::types::BlockStore;
use dag_service::{self, add_entries, Entry};
use data_transfer::{DataTransfer, DealParams};
use graphsync::traversal::unixfs_path_selector;
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use libp2p::{
    core,
    core::muxing::StreamMuxerBox,
    core::transport::{Boxed, OptionalTransport},
    dns, identity, mplex, noise, tcp, websocket, Multiaddr, PeerId, Swarm, Transport,
};
use parking_lot::Mutex;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{fs, io};
use warp::{http, reject::Reject, Rejection};

#[derive(Debug, Clone)]
enum Failure {
    InvalidCid { err: String },
    ReadFailure { err: String },
    WriteFailure { err: String },
    InvalidPath { err: String },
    InvalidPeerId { err: String },
    InvalidMultiAdd { err: String },
    TransferFailed { err: String },
}
impl Reject for Failure {}
impl Failure {
    fn get_err(self) -> String {
        match self {
            Failure::InvalidCid { err } => err,
            Failure::ReadFailure { err } => err,
            Failure::WriteFailure { err } => err,
            Failure::InvalidPath { err } => err,
            Failure::InvalidPeerId { err } => err,
            Failure::InvalidMultiAdd { err } => err,
            Failure::TransferFailed { err } => err,
        }
    }
}

// Custom rejection handler that maps rejections into responses.
pub async fn handle_rejection(
    err: Rejection,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    if err.is_not_found() {
        Ok(warp::reply::with_status(
            "NOT_FOUND".to_string(),
            http::StatusCode::NOT_FOUND,
        ))
    } else if let Some(e) = err.find::<crate::native::server::Failure>() {
        let resp = format!("request failed: {}", e.clone().get_err());
        Ok(warp::reply::with_status(
            resp,
            http::StatusCode::BAD_REQUEST,
        ))
    } else {
        eprintln!("unhandled rejection: {:?}", err);
        Ok(warp::reply::with_status(
            "INTERNAL_SERVER_ERROR".to_string(),
            http::StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}

pub async fn read_file<B: BlockStore>(
    path: String,
    store: Arc<B>,
) -> Result<impl warp::Reply, warp::Rejection>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    let metadata = fs::metadata(path.clone())
        .map_err(|e| warp::reject::custom(Failure::InvalidPath { err: e.to_string() }))?;
    if metadata.is_file() {
        let mut f = fs::File::open(path)
            .map_err(|e| warp::reject::custom(Failure::InvalidPath { err: e.to_string() }))?;
        let result = dag_service::add_from_read(store.clone(), &mut f)
            .map_err(|e| warp::reject::custom(Failure::ReadFailure { err: e.to_string() }))?;
        return result
            .and_then(|(root, size)| {
                // map the result to another result containing a simple string
                Some(format!(
                    "added file {:?} ({}bytes) to blockstore",
                    root, size
                ))
            })
            .map(|resp| warp::reply::with_status(resp, http::StatusCode::CREATED))
            .ok_or(warp::reject::not_found());
    }
    if metadata.is_dir() {
        return fs::read_dir(path)
            .and_then(|paths| {
                // if reading the directory is successful open all the file entries
                // if one of the file fails to open we just ignore it
                let files: Vec<(String, fs::File)> = paths
                    .filter_map(|p| {
                        p.and_then(|e| {
                            let file = fs::File::open(e.path())?;
                            Ok((e.file_name().into_string().unwrap(), file))
                        })
                        .ok()
                    })
                    .collect();
                let (cid, size) = add_entries(
                    store,
                    files
                        .iter()
                        .map(|(n, r)| Entry {
                            name: n.to_string(),
                            reader: r,
                        })
                        .collect(),
                )
                .map_err(|s| io::Error::new(io::ErrorKind::Other, s))?;
                Ok(warp::reply::with_status(
                    format!("added dir {:?} ({}bytes) to blockstore", cid, size),
                    http::StatusCode::CREATED,
                ))
            })
            .map_err(|e| warp::reject::custom(Failure::ReadFailure { err: e.to_string() }));
    }
    Err(warp::reject::not_found())
}

pub async fn export_file<B: BlockStore>(
    key: String,
    path: String,
    store: Arc<B>,
) -> Result<impl warp::Reply, warp::Rejection>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    let cid = Cid::try_from(key)
        .map_err(|e| warp::reject::custom(Failure::InvalidCid { err: e.to_string() }))?;
    let file = fs::File::create(path).unwrap();
    dag_service::cat_to_write(store.clone(), cid, file)
        .map_err(|e| warp::reject::custom(Failure::WriteFailure { err: e.to_string() }))?;
    let resp = format!("loaded file {:?} from blockstore", cid.to_string());
    Ok(warp::reply::with_status(resp, http::StatusCode::CREATED))
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
    let (cid, selector) =
        unixfs_path_selector(key).ok_or(warp::reject::custom(Failure::InvalidCid {
            err: "Failed to parse ipfs path".to_string(),
        }))?;
    let peer = PeerId::from_str(&peer)
        .map_err(|e| warp::reject::custom(Failure::InvalidPeerId { err: e.to_string() }))?;
    let multiaddr = Multiaddr::try_from(multiaddr)
        .map_err(|e| warp::reject::custom(Failure::InvalidMultiAdd { err: e.to_string() }))?;

    let mut lock = swarm.lock();
    lock.behaviour_mut().add_address(&peer, multiaddr);
    lock.behaviour_mut()
        .pull(peer, cid, selector, DealParams::default())
        .map_err(|e| warp::reject::custom(Failure::TransferFailed { err: e.to_string() }))?;

    let resp = format!("transfer started in background");
    Ok(warp::reply::with_status(resp, http::StatusCode::OK))
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
