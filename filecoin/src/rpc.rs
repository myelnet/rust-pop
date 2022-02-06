use jsonrpc_v2::{Error, Id, RequestObject, V2};
use log::error;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Debug;

pub const API_INFO_KEY: &str = "FULLNODE_API_INFO";
pub const DEFAULT_URL: &str = "https://infura.myel.cloud";

/// Message Pool API
pub mod mpool_api {
    use crate::types::signed_json::SignedMessageJson;
    use cid::json::CidJson;

    pub const MPOOL_PUSH: &str = "Filecoin.MpoolPush";
    pub type MpoolPushParams = (SignedMessageJson,);
    pub type MpoolPushResult = CidJson;
}

/// Error object in a response
#[derive(Debug, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcResponse<R> {
    Result {
        jsonrpc: V2,
        result: R,
        id: Id,
    },
    Error {
        jsonrpc: V2,
        error: JsonRpcError,
        id: Id,
    },
}

pub struct Client {
    endpoint: String,
}

impl Client {
    pub fn new() -> Client {
        let api_url = env::var(API_INFO_KEY).unwrap_or_else(|_| DEFAULT_URL.to_owned());
        Client { endpoint: api_url }
    }

    pub async fn mpool_push(
        &mut self,
        params: mpool_api::MpoolPushParams,
    ) -> Result<mpool_api::MpoolPushResult, Error> {
        self.call(mpool_api::MPOOL_PUSH, params).await
    }
    /// Utility method for sending RPC requests over HTTP
    async fn call<P, R>(&mut self, method_name: &str, params: P) -> Result<R, Error>
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
    {
        let rpc_req = RequestObject::request()
            .with_method(method_name)
            .with_params(serde_json::to_value(&params)?)
            .finish();

        let mut http_res = surf::post(&self.endpoint)
            .content_type("application/json-rpc")
            .body(surf::Body::from_json(&rpc_req)?)
            .await?;

        let res = http_res.body_string().await?;

        let code = http_res.status() as i64;


        if code != 200 {
            return Err(Error::Full {
                message: format!("Error code from HTTP Response: {}", code),
                code,
                data: None,
            });
        }

        // Return the parsed RPC result
        let rpc_res: JsonRpcResponse<R> = match serde_json::from_str(&res) {
            Ok(r) => r,
            Err(e) => {
                let err = format!(
                    "Parse Error: Response from RPC endpoint could not be parsed. Error was: {}",
                    e
                );
                error!("{}", &err);
                return Err(err.into());
            }
        };

        match rpc_res {
            JsonRpcResponse::Result { result, .. } => Ok(result),
            JsonRpcResponse::Error { error, .. } => Err(Error::Full {
                data: None,
                code: error.code,
                message: error.message,
            }),
        }
    }
}
