use libp2p::core::PeerId;
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    Gossipsub, GossipsubConfigBuilder, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity,
    ValidationMode,
};
use std::time::Duration;

pub trait ShrinkableMap<Q: ?Sized, V> {
    fn remove(&mut self, k: &Q) -> Option<V>;
    fn contains_key(&self, k: &Q) -> bool;
    }


pub fn gossip_init(peer_id: PeerId, topics: Vec<Topic>) -> Gossipsub {
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

    for topic in topics {
        content_routing.subscribe(&topic).unwrap();
    }
    content_routing
}
