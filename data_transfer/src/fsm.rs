use super::{ChannelId, DataTransferEvent};

#[derive(Debug, PartialEq, Clone)]
pub enum Channel {
    New {
        id: ChannelId,
        deal_id: u64,
    },
    Accepted {
        id: ChannelId,
        deal_id: u64,
    },
    Ongoing {
        id: ChannelId,
        received: usize,
        all_received: bool,
    },
    PendingLastBlocks {
        id: ChannelId,
        received: usize,
    },
    Failed {
        id: ChannelId,
        reason: String,
    },
    Completed {
        id: ChannelId,
        received: usize,
    },
}

#[derive(Debug, Clone)]
pub enum ChannelEvent {
    Accepted,
    BlockReceived { size: usize },
    AllBlocksReceived,
    Failure { reason: String },
    Completed,
}

impl Channel {
    pub fn transition(self, event: ChannelEvent) -> Channel {
        match (self, event) {
            (Channel::New { id, .. }, ChannelEvent::BlockReceived { size }) => Channel::Ongoing {
                id,
                received: size,
                all_received: false,
            },
            (Channel::New { id, .. }, ChannelEvent::Failure { reason }) => {
                Channel::Failed { id, reason }
            }
            (Channel::New { id, deal_id }, ChannelEvent::Accepted) => {
                Channel::Accepted { id, deal_id }
            }
            (Channel::Accepted { id, .. }, ChannelEvent::BlockReceived { size }) => {
                Channel::Ongoing {
                    id,
                    received: size,
                    all_received: false,
                }
            }
            (Channel::Accepted { id, .. }, ChannelEvent::Completed) => {
                Channel::PendingLastBlocks { id, received: 0 }
            }
            (
                Channel::Ongoing {
                    id,
                    received,
                    all_received,
                },
                ChannelEvent::BlockReceived { size },
            ) => Channel::Ongoing {
                id,
                received: received + size,
                all_received,
            },
            (Channel::Ongoing { id, received, .. }, ChannelEvent::AllBlocksReceived) => {
                Channel::Ongoing {
                    id,
                    received,
                    all_received: true,
                }
            }
            (
                Channel::Ongoing {
                    id,
                    received,
                    all_received,
                },
                ChannelEvent::Completed,
            ) => {
                if all_received {
                    Channel::Completed { id, received }
                } else {
                    Channel::PendingLastBlocks { id, received }
                }
            }
            (Channel::Ongoing { id, .. }, ChannelEvent::Failure { reason }) => {
                Channel::Failed { id, reason }
            }
            (Channel::PendingLastBlocks { id, received }, ChannelEvent::BlockReceived { size }) => {
                Channel::PendingLastBlocks {
                    id,
                    received: received + size,
                }
            }
            (Channel::PendingLastBlocks { id, received }, ChannelEvent::AllBlocksReceived) => {
                Channel::Completed { id, received }
            }
            (Channel::PendingLastBlocks { id, .. }, ChannelEvent::Failure { reason }) => {
                Channel::Failed { id, reason }
            }
            (s, e) => {
                panic!("Invalid state transition: {:#?} {:#?}", s, e)
            }
        }
    }
}

impl From<Channel> for DataTransferEvent {
    fn from(ch: Channel) -> DataTransferEvent {
        match ch {
            Channel::New { id, .. } => DataTransferEvent::Started(id),
            Channel::Accepted { id, .. } => DataTransferEvent::Accepted(id),
            Channel::Ongoing {
                id,
                received: _,
                all_received: _,
            } => DataTransferEvent::Progress(id),
            Channel::PendingLastBlocks { id, received: _ } => DataTransferEvent::Progress(id),
            Channel::Failed { id, reason } => DataTransferEvent::Completed(id, Err(reason)),
            Channel::Completed { id, received: _ } => DataTransferEvent::Completed(id, Ok(())),
        }
    }
}
