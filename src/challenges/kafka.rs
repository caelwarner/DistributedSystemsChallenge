use std::collections::HashMap;
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use distributed_systems_challenge::{Message, MessageReply, Node, NodeServer};

#[tokio::main]
async fn main() -> Result<()> {
    NodeServer::new(State::default(), message_handler)
        .serve()
        .await
}

async fn message_handler(node: Node<State, Payload>, message: Message<Payload>) -> MessageReply<Payload> {
    let (message, payload) = message.take_payload();

    Ok(match payload {
        Payload::Send { key, log_message } => {
            let logs = &mut node.state.write().unwrap().logs;

            let log = logs.entry(key).or_default();
            log.messages.push(log_message);

            Some(message.into_reply(Payload::SendOk { offset: log.messages.len() - 1 }))
        },
        Payload::Poll { offsets } => {
            let logs = &node.state.read().unwrap().logs;

            let log_messages = offsets.into_iter()
                .map(|(key, offset)| {
                    let log_messages = logs.get(&key).unwrap().messages.iter()
                        .copied()
                        .enumerate()
                        .skip(offset)
                        .collect::<Vec<_>>();

                    (key, log_messages)
                })
                .collect::<HashMap<_, _>>();

            Some(message.into_reply(Payload::PollOk { log_messages }))
        },
        Payload::CommitOffsets { offsets } => {
            let logs = &mut node.state.write().unwrap().logs;

            for (key, offset) in offsets {
                logs.get_mut(&key).unwrap().committed_offset = offset;
            }

            Some(message.into_reply(Payload::CommitOffsetsOk))
        },
        Payload::ListCommittedOffsets { keys } => {
            let logs = &node.state.read().unwrap().logs;

            let offsets = keys.into_iter()
                .map(|key| {
                    let offset = logs.get(&key).unwrap().committed_offset;
                    (key, offset)
                })
                .collect::<HashMap<_, _>>();

            Some(message.into_reply(Payload::ListCommittedOffsetsOk { offsets }))
        },
        Payload::SendOk { .. } | Payload::PollOk { .. } | Payload::CommitOffsetsOk | Payload::ListCommittedOffsetsOk { .. } => {
            None
        },
    })
}

#[derive(Default)]
struct State {
    logs: HashMap<String, Log>,
}

#[derive(Default)]
struct Log {
    messages: Vec<i32>,
    committed_offset: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        #[serde(rename = "msg")]
        log_message: i32,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        #[serde(rename = "msgs")]
        log_messages: HashMap<String, Vec<(usize, i32)>>
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}
