use blockstore::db::Db as BlockstoreDB;
use blockstore::lfu::LfuBlockstore;
use clap::{Arg, Command};
// use libipld::Cid;
// // use libp2p::{Multiaddr, PeerId};
use pop::{Node, NodeConfig};
use std::collections::HashMap;
use std::error::Error;
// use std::str::FromStr;

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let app = Command::new("PoP")
        .author("Myel")
        .version("0.0.1")
        .about("Runs a cache provider node on the Myel network")
        .subcommand(
            Command::new("start")
                .override_help("starts a pop node")
                .arg(
                    Arg::new("peers")
                        .short('p')
                        .long("peers")
                        .help("peer multi-addresses to connect to from start"),
                ),
        )
        .subcommand(Command::new("info").override_help("print info about the running node"))
        .subcommand(
            Command::new("add")
                .override_help("adds a local file to pop node blockstore")
                .arg(
                    Arg::new("path")
                        .short('p')
                        .takes_value(true)
                        .long("pathname")
                        .required(true)
                        .help("path of the file or directory to add to node blockstore"),
                ),
        )
        .subcommand(
            Command::new("import")
                .override_help("import a DAG from a CAR file into the blockstore")
                .arg(
                    Arg::new("path")
                        .short('p')
                        .takes_value(true)
                        .long("pathname")
                        .required(true)
                        .help("path to the CAR file"),
                ),
        )
        .subcommand(
            Command::new("export")
                .override_help("gets a file from blockstore")
                .arg(
                    Arg::new("cid")
                        .short('c')
                        .takes_value(true)
                        .long("cid")
                        .required(true)
                        .help("cid to export"),
                )
                .arg(
                    Arg::new("path")
                        .short('p')
                        .takes_value(true)
                        .long("path")
                        .required(true)
                        .help("path to save file to"),
                ),
        )
        .subcommand(
            Command::new("retrieve")
                .override_help("gets a file from blockstore")
                .arg(
                    Arg::new("path")
                        .short('p')
                        .takes_value(true)
                        .long("ipfspath")
                        .required(true)
                        .help("ipfs path to resolve the content"),
                )
                .arg(
                    Arg::new("peer")
                        .short('p')
                        .takes_value(true)
                        .long("peer")
                        .required(true)
                        .help("peer id to fetch from"),
                )
                .arg(
                    Arg::new("multiaddr")
                        .short('m')
                        .takes_value(true)
                        .long("multiaddr")
                        .required(true)
                        .help("multiaddr of peer to fetch from"),
                ),
        );

    let matches = app.get_matches();

    match matches.subcommand_name() {
        Some("start") => start().await,
        Some("info") => info().await,
        Some("add") => {
            let flags = matches.subcommand().unwrap().1;
            //  can safely unwrap subcommand because we have just checked its name
            add(flags.values_of("path").unwrap().collect()).await
        }
        Some("import") => {
            let flags = matches.subcommand().unwrap().1;
            import(flags.values_of("path").unwrap().collect()).await
        }
        Some("export") => {
            let flags = matches.subcommand().unwrap().1;
            export(
                flags.values_of("cid").unwrap().collect(),
                flags.values_of("path").unwrap().collect(),
            )
            .await
        }
        Some("retrieve") => {
            let flags = matches.subcommand().unwrap().1;
            retrieve(
                flags.values_of("path").unwrap().collect(),
                flags.values_of("peer").unwrap().collect(),
                flags.values_of("multiaddr").unwrap().collect(),
            )
            .await
        }
        _ => unreachable!("parser should ensure only valid subcommand names are used"),
    }
}

async fn info() -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    let resp = client.post("http:://127.0.0.1:27403/info").send().await?;
    println!("{:?}: {:?}", resp.status(), resp.text().await.unwrap());
    Ok(())
}

async fn add(path: String) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    let resp = client
        .post("http://127.0.0.1:27403/add")
        .body(path)
        .send()
        .await?;
    println!("{:?}: {:?}", resp.status(), resp.text().await.unwrap());
    Ok(())
}

async fn import(path: String) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    let resp = client
        .post("http://127.0.0.1:27403/import")
        .body(path)
        .send()
        .await?;
    println!("{:?}: {:?}", resp.status(), resp.text().await.unwrap());
    Ok(())
}

async fn export(cid: String, path: String) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    let map = HashMap::from([("cid", cid), ("path", path)]);
    let resp = client
        .post("http://127.0.0.1:27403/export")
        .json(&map)
        .send()
        .await?;
    println!("{:?}: {:?}", resp.status(), resp.text().await.unwrap());
    Ok(())
}

async fn retrieve(path: String, peer: String, multiaddr: String) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    let map = HashMap::from([("path", path), ("peer", peer), ("multiaddr", multiaddr)]);
    let resp = client
        .post("http://127.0.0.1:27403/retrieve")
        .json(&map)
        .send()
        .await?;
    println!("{:?}: {:?}", resp.status(), resp.text().await.unwrap());
    Ok(())
}

async fn start() -> Result<(), Box<dyn Error>> {
    let bs = LfuBlockstore::new(0, BlockstoreDB::open("blocks")?)?;
    let config = NodeConfig {
        listening_multiaddr: Some("/ip4/0.0.0.0/tcp/0/ws".parse()?),
        blockstore: bs,
    };

    let mut node = Node::new(config);
    node.run().await;

    Ok(())
}
