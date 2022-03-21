#[derive(Debug, PartialEq, Clone)]
pub enum DtState {
    New,
    Accepted,
    Ongoing { received: usize, all_received: bool },
    PendingLastBlocks { received: usize },
    Failed { reason: String },
    Completed { received: usize },
}

#[derive(Debug, Clone)]
pub enum DtEvent {
    Accepted,
    BlockReceived { size: usize },
    AllBlocksReceived,
    Failure { reason: String },
    Completed,
}

impl DtState {
    pub fn transition(self, event: DtEvent) -> DtState {
        match (self, event) {
            (DtState::New, DtEvent::BlockReceived { size }) => DtState::Ongoing {
                received: size,
                all_received: false,
            },
            (DtState::New, DtEvent::Failure { reason }) => DtState::Failed { reason },
            (DtState::New, DtEvent::Accepted) => DtState::Accepted,
            (DtState::New, DtEvent::Completed) => DtState::PendingLastBlocks { received: 0 },
            (DtState::Accepted, DtEvent::BlockReceived { size }) => DtState::Ongoing {
                received: size,
                all_received: false,
            },
            (DtState::Accepted, DtEvent::Completed) => DtState::PendingLastBlocks { received: 0 },
            (
                DtState::Ongoing {
                    received,
                    all_received,
                },
                DtEvent::BlockReceived { size },
            ) => DtState::Ongoing {
                received: received + size,
                all_received,
            },
            (DtState::Ongoing { received, .. }, DtEvent::AllBlocksReceived) => DtState::Ongoing {
                received,
                all_received: true,
            },
            (
                DtState::Ongoing {
                    received,
                    all_received,
                },
                DtEvent::Completed,
            ) => {
                if all_received {
                    DtState::Completed { received }
                } else {
                    DtState::PendingLastBlocks { received }
                }
            }
            (DtState::Ongoing { .. }, DtEvent::Failure { reason }) => DtState::Failed { reason },
            (DtState::PendingLastBlocks { received }, DtEvent::BlockReceived { size }) => {
                DtState::PendingLastBlocks {
                    received: received + size,
                }
            }
            (DtState::PendingLastBlocks { received }, DtEvent::AllBlocksReceived) => {
                DtState::Completed { received }
            }
            (DtState::PendingLastBlocks { .. }, DtEvent::Failure { reason }) => {
                DtState::Failed { reason }
            }
            (s, e) => {
                panic!("Invalid state transition: {:#?} {:#?}", s, e)
            }
        }
    }
}
