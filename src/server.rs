use blockstore::types::BlockStore;
use dag_service;
use libipld::codec::Decode;
use libipld::store::StoreParams;
use libipld::Ipld;
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
    // serve on port 8000
    warp::serve(add_file).run(([127, 0, 0, 1], 8000)).await;
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
                Err(e) => {
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
