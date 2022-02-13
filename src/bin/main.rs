use blockstore::db::Db as BlockstoreDB;
use blockstore::lfu::LfuBlockstore;
use blockstore::types::BlockStore;
use clap::{App, Arg};
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::Cid;
use libipld::Ipld;
use libp2p::{Multiaddr, PeerId};
use pop::{Node, NodeConfig};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use warp::{http, Filter}; // 0.3.5
                                                    // #[tokio::main]
#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let app = App::new("PoP")
        .author("Myel")
        .version("0.0.1")
        .about("Runs a cache provider node on the Myel network")
        .subcommand(
            App::new("start").override_help("starts a pop node").arg(
                Arg::new("peers")
                    .short('p')
                    .long("peers")
                    .help("peer multi-addresses to connect to from start"),
            ),
        )
        .subcommand(
            App::new("add")
                .override_help("adds a local file to pop node blockstore")
                .arg(
                    Arg::new("file")
                        .short('f')
                        .takes_value(true)
                        .long("file")
                        .required(true)
                        .help("file to add to node blockstore"),
                ),
        );

    let matches = app.get_matches();

    println!("{:?}", matches);

    match matches.subcommand_name() {
        Some("start") => start().await,
        Some("add") => {
            //  can safely unwrap subcommand because we have just checked its name
            add(matches
                .subcommand()
                .unwrap()
                .1
                .values_of("file")
                .unwrap()
                .collect())
            .await
        }
        _ => unreachable!("parser should ensure only valid subcommand names are used"),
    }
}

async fn add(path: String) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    client
        .post("http://127.0.0.1:2021/add")
        .body(path)
        .send()
        .await?;
    Ok(())
}

async fn start() -> Result<(), Box<dyn Error>> {
    let bs = LfuBlockstore::new(0, BlockstoreDB::open("path")?)?;
    let config = NodeConfig {
        listening_multiaddr: "/ip4/0.0.0.0/tcp/0/ws".parse()?,
        wasm_external_transport: None,
        blockstore: bs,
    };

    let mut node = Node::new(config);

    if let Some(addr) = std::env::args().nth(2) {
        let remote: Multiaddr = addr.parse()?;
        if let Some(peer) = std::env::args().nth(3) {
            let peer_id = PeerId::from_str(&peer)?;
            if let Some(key) = std::env::args().nth(4) {
                let cid = Cid::try_from(key)?;
                node.run_request(remote, peer_id, cid).await;
            }
        }
    } else {
        node.fill_random_data();
    }

    let arc_ref = Arc::clone(&bs);
    let add_file = warp::post()
        .and(warp::path("add"))
        .and(warp::body::bytes())
        .map(|bytes: warp::hyper::body::Bytes| {
            return std::str::from_utf8(&bytes).unwrap().to_string();
        })
        .map(move |s: String| {
            read_file(arc_ref.clone(), &s);
            warp::reply::with_status("Added file to the blockstore", http::StatusCode::CREATED)
        });
    //
    let server = warp::serve(add_file);
    println!("MAde it hereeee");

    futures::future::join(node.run(), server.run(([127, 0, 0, 1], 2021))).await;

    Ok(())
}
