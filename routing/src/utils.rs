use crate::hub_discovery::{PeerTable, SerializablePeerTable};
use crate::hub_indexing::{ContentTable, SerializableContentTable};
use filecoin::cid_helpers::CidCbor;
use libipld::Cid;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    Gossipsub, GossipsubConfigBuilder, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity,
    ValidationMode,
};
use smallvec::SmallVec;
use std::time::Duration;

pub fn peer_table_to_bytes(p: &PeerTable) -> SerializablePeerTable {
    p.iter()
        .map_while(|(peer, addresses)| {
            let mut addr_vec = Vec::new();
            for addr in addresses {
                addr_vec.push((*addr).to_vec())
            }

            Some((peer.to_bytes(), addr_vec))
        })
        .collect()
}

pub fn peer_table_from_bytes(p: &SerializablePeerTable) -> PeerTable {
    //  addresses only get returned on a successful response
    p.iter()
        .map_while(|(peer, addresses)| {
            let mut addr_vec = SmallVec::<[Multiaddr; 6]>::new();
            // check sent peer is valid
            match PeerId::from_bytes(peer) {
                Ok(p) => {
                    // check associated multiaddresses are valid
                    for addr in addresses {
                        if let Ok(a) = Multiaddr::try_from(addr.clone()) {
                            addr_vec.push(a)
                        }
                    }
                    // remove any potential duplicate data
                    addr_vec.sort();
                    addr_vec.dedup();
                    Some((p, addr_vec))
                }
                Err(_) => None,
            }
        })
        .collect()
}

pub fn content_table_to_bytes(c: &ContentTable) -> SerializableContentTable {
    c.iter()
        .map_while(|(cid, addresses)| {
            let peer_table = peer_table_to_bytes(addresses);
            Some((cid.bytes().clone(), peer_table))
        })
        .collect()
}

pub fn content_table_from_bytes(c: &SerializableContentTable) -> ContentTable {
    c.iter()
        .map_while(|(cid, addresses)| {
            // check sent peer is valid
            match Cid::try_from(cid.clone()) {
                Ok(c) => {
                    let peer_table = peer_table_from_bytes(addresses);
                    Some((CidCbor::from(c), peer_table))
                }
                Err(_) => None,
            }
        })
        .collect()
}

pub fn content_routing_init(peer_id: PeerId, topic: Topic) -> Gossipsub {
    // We take current time as request id as request content may not be unique
    let message_id_fn = |_message: &GossipsubMessage| MessageId::from(instant::now().to_ne_bytes());

    // Set a custom gossipsub
    let gossipsub_config = GossipsubConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
        .validation_mode(ValidationMode::Permissive) // This sets the kind of message validation. The default is Strict (enforce message signing)
        .message_id_fn(message_id_fn) // content-address messages. No two messages of the
        // same content will be propagated.
        .build()
        .expect("Valid config");
    // build a gossipsub network behaviour
    let mut content_routing: Gossipsub =
        Gossipsub::new(MessageAuthenticity::Author(peer_id), gossipsub_config)
            .expect("Correct configuration");

    // subscribes to our topic
    content_routing.subscribe(&topic).unwrap();
    content_routing
}
