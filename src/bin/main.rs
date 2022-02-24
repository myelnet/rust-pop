use blockstore::db::Db as BlockstoreDB;
use blockstore::lfu::LfuBlockstore;
use clap::{App, Arg};
// use libipld::Cid;
// // use libp2p::{Multiaddr, PeerId};
use pop::{Node, NodeConfig};
use std::collections::HashMap;
use std::error::Error;
// use std::str::FromStr;

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
        )
        .subcommand(
            // TODO: implement get method on cli
            App::new("get")
                .override_help("gets a file from a peer")
                // .arg(
                //     Arg::new("peer")
                //         .short('p')
                //         .takes_value(true)
                //         .long("file")
                //         .required(false)
                //         .help("path to save file to"),
                // )
                .arg(
                    Arg::new("path")
                        .short('f')
                        .takes_value(true)
                        .long("file")
                        .required(false)
                        .help("path to save file to"),
                ),
        )
        .subcommand(
            App::new("export")
                .override_help("gets a file from blockstore")
                .arg(
                    Arg::new("cid")
                        .short('c')
                        .takes_value(true)
                        .long("cid")
                        .required(true)
                        .help("path to save file to"),
                )
                .arg(
                    Arg::new("path")
                        .short('p')
                        .takes_value(true)
                        .long("path")
                        .required(true)
                        .help("path to save file to"),
                ),
        );

    let matches = app.get_matches();

    match matches.subcommand_name() {
        Some("start") => start().await,
        Some("add") => {
            let flags = matches.subcommand().unwrap().1;
            //  can safely unwrap subcommand because we have just checked its name
            add(flags.values_of("file").unwrap().collect()).await
        }
        Some("export") => {
            let flags = matches.subcommand().unwrap().1;
            export(
                flags.values_of("cid").unwrap().collect(),
                flags.values_of("path").unwrap().collect(),
            )
            .await
        }
        _ => unreachable!("parser should ensure only valid subcommand names are used"),
    }
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

async fn start() -> Result<(), Box<dyn Error>> {
    let bs = LfuBlockstore::new(0, BlockstoreDB::open("path")?)?;
    let config = NodeConfig {
        listening_multiaddr: Some("/ip4/0.0.0.0/tcp/0/ws".parse()?),
        blockstore: bs,
    };

    let node = Node::new(config).await;

    node.run().await;

    Ok(())
}
