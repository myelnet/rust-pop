use super::{DiscoveryResponse, RequestId, SerializablePeerTable};

#[derive(Debug)]
pub struct ResponseBuilder {
    req_id: RequestId,
}

impl ResponseBuilder {
    pub fn new(req_id: RequestId) -> Self {
        Self { req_id }
    }

    pub fn completed(self, addresses: SerializablePeerTable) -> DiscoveryResponse {
        self.send(Some(addresses))
    }

    fn send(self, addresses: Option<SerializablePeerTable>) -> DiscoveryResponse {
        let msg = DiscoveryResponse {
            id: self.req_id,
            addresses,
        };

        return msg;
    }
}
