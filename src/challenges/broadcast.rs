use std::collections::{HashMap, HashSet};
use std::time::Duration;

use color_eyre::eyre::{OptionExt, Result};
use serde::{Deserialize, Serialize};

use distributed_systems_challenge::{Message, MessageReply, Node, NodeId, NodeServer};

#[tokio::main]
async fn main() -> Result<()> {
    NodeServer::new(State::default(), message_handler)
        .add_task(sync_with_neighbours, Duration::from_millis(450))
        .serve()
        .await
}

async fn message_handler(node: Node<State, Payload>, message: Message<Payload>) -> MessageReply<Payload> {
    let (message, payload) = message.take_payload();

    Ok(match payload {
        Payload::Broadcast { broadcast_message } => {
            node.state.write().unwrap().broadcast_messages.insert(broadcast_message);
            Some(message.into_reply(Payload::BroadcastOk))
        },
        Payload::Read => {
            Some(message.into_reply(Payload::ReadOk {
                broadcast_messages: node.state.read().unwrap().broadcast_messages.clone(),
            }))
        },
        Payload::Topology { .. } => {
            let mut state = node.state.write().unwrap();
            // let neighbours = topology.remove(&node.node_id).ok_or_eyre("Topology missing node of self")?;
            //
            // for neighbour in neighbours {
            //     state.neighbours.insert(
            //         neighbour,
            //         HashSet::new(),
            //     );
            // }

            // All node communicate with all other nodes for low latency
            for node in node.node_ids.clone() {
                state.neighbours.insert(node, HashSet::new());
            }

            Some(message.into_reply(Payload::TopologyOk))
        },
        Payload::Sync { new_messages } => {
            let mut state = node.state.write().unwrap();
            let src = message.src.clone();

            state.neighbours.get_mut(&src)
                .ok_or_eyre("Unknown neighbour")?
                .extend(new_messages.clone());
            state.broadcast_messages.extend(new_messages.clone());

            let reply = message.into_reply(Payload::SyncOk { acknowledge_new_messages: new_messages });

            Some(reply)
        },
        Payload::SyncOk { acknowledge_new_messages } => {
            let mut state = node.state.write().unwrap();

            state.neighbours.get_mut(&message.src)
                .ok_or_eyre("Unknown neighbour")?
                .extend(acknowledge_new_messages);

            None
        },
        Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {
            None
        },
    })
}

async fn sync_with_neighbours(node: Node<State, Payload>) -> Result<()> {
    let messages = {
        let state = node.state.read().unwrap();

        state.neighbours.iter()
            .map(|(neighbour_id, messages_known)| {
                let messages_to_send = state.broadcast_messages.difference(&messages_known)
                    .copied()
                    .collect::<HashSet<_>>();

                (neighbour_id.clone(), messages_to_send)
            })
            .filter(|(_, messages_to_send)| !messages_to_send.is_empty())
            .collect::<Vec<_>>()
    };

    for (dest, messages_to_send) in messages {
        node.send_new_message(dest, Payload::Sync {
            new_messages: messages_to_send,
        }).await?;
    }

    Ok(())
}

type MessagesKnown = HashSet<i32>;

#[derive(Default)]
struct State {
    neighbours: HashMap<NodeId, MessagesKnown>,
    broadcast_messages: HashSet<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Broadcast {
        #[serde(rename = "message")]
        broadcast_message: i32,
    },
    BroadcastOk,
    Read,
    ReadOk {
        #[serde(rename = "messages")]
        broadcast_messages: HashSet<i32>,
    },
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    TopologyOk,
    Sync {
        new_messages: HashSet<i32>,
    },
    SyncOk {
        acknowledge_new_messages: HashSet<i32>,
    },
}
