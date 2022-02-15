use blockstore::types::BlockStore;
use dag_service;
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::{Cid, Ipld};
use std::collections::HashMap;
use std::fs::File;
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
            return std::str::from_utf8(&bytes).unwrap().to_string();
        })
        .and(store_filter.clone())
        .and_then(|path: String, store: Arc<B>| {
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
                simple_map.get("cid").unwrap().to_string(),
                simple_map.get("path").unwrap().to_string(),
                store.clone(),
            );
        });

    let routes = add_file.or(export_file);
    // serve on port 3000
    async_std::task::spawn(async move { warp::serve(routes).run(([127, 0, 0, 1], 3000)).await });
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
