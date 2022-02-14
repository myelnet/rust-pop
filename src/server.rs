use blockstore::types::BlockStore;
use dag_service;
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use std::collections::HashMap;
use std::fs::File;
use std::ops::Deref;
use std::sync::Arc;
use warp::{http, Filter};

pub async fn start_server<B: 'static + BlockStore>(store: Arc<B>)
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    let store_filter = warp::any().map(move || store.clone());

    let add_file = warp::post()
        .and(warp::path("add"))
        .and(warp::body::bytes())
        .map(|bytes: warp::hyper::body::Bytes| {
            return Arc::new(std::str::from_utf8(&bytes).unwrap().to_string());
        })
        .and(store_filter.clone())
        .and_then(|path: Arc<String>, store: Arc<B>| {
            return read_file(path.clone(), store.clone());
        });

    let export_file = warp::post()
        .and(warp::path("export"))
        .and(warp::body::json())
        .and(store_filter.clone())
        .and_then(|simple_map: HashMap<String, String>, store: Arc<B>| {
            //  can safely unwrap entries as if they are None the method will just return a failure
            //  response to the requesting client
            return export_file(
                Arc::new(simple_map.get("cid").unwrap().to_string()),
                Arc::new(simple_map.get("path").unwrap().to_string()),
                store.clone(),
            );
        });

    let routes = add_file.or(export_file);
    // serve on port 8000
    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
}

pub async fn read_file<B: BlockStore>(
    path: Arc<String>,
    store: Arc<B>,
) -> Result<impl warp::Reply, warp::Rejection>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    match File::open((*path).deref()) {
        Ok(mut f) => {
            let res = dag_service::add_from_read(store.clone(), &mut f);
            match res {
                Ok(root) => {
                    println!("added file {:?} to blockstore", root.unwrap().to_string());
                    Ok(warp::reply::with_status(
                        "Added file to the blockstore",
                        http::StatusCode::CREATED,
                    ))
                }
                Err(_) => {
                    println!("failed to read buffer");
                    Err(warp::reject::not_found())
                }
            }
        }
        Err(_) => {
            println!("could not find file");
            Err(warp::reject::not_found())
        }
    }
}

pub async fn export_file<B: BlockStore>(
    key: Arc<String>,
    path: Arc<String>,
    store: Arc<B>,
) -> Result<impl warp::Reply, warp::Rejection>
where
    Ipld: Decode<<<B as BlockStore>::Params as StoreParams>::Codecs>,
{
    match Cid::try_from((*key).deref()) {
        Ok(cid) => {
            let file = File::create((*path).deref()).unwrap();
            let res = dag_service::cat_to_write(store.clone(), cid, file);
            match res {
                Ok(_) => {
                    println!("loaded file {:?} from blockstore", cid.to_string());
                    Ok(warp::reply::with_status(
                        "read file from blockstore",
                        http::StatusCode::CREATED,
                    ))
                }
                Err(_) => {
                    println!("failed to read from blockstore");
                    Err(warp::reject::not_found())
                }
            }
        }
        Err(_) => {
            println!("invalid cid");
            Err(warp::reject::not_found())
        }
    }
}
