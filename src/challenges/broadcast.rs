use std::collections::{HashMap, HashSet};
use std::time::Duration;

use color_eyre::eyre::{OptionExt, Result};
use serde::{Deserialize, Serialize};

use gossip_glomers::{Message, MessageReply, Node, NodeId, NodeServer};

#[tokio::main]
async fn main() -> Result<()> {
    NodeServer::new(State::default(), message_handler)
        .add_task(sync_with_neighbours, Duration::from_millis(500))
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
        Payload::Topology { mut topology } => {
            let neighbours = topology.remove(&node.node_id).ok_or_eyre("Topology missing node of self")?;
            let mut state = node.state.write().unwrap();

            for neighbour in neighbours {
                state.neighbours.push(NeighbourState {
                    id: neighbour,
                    broadcast_messages: HashSet::new(),
                });
            }

            Some(message.into_reply(Payload::TopologyOk))
        },
        Payload::Sync { broadcast_messages: neighbours_messages } => {
            let mut state = node.state.write().unwrap();

            let neighbour_index = state.neighbours.iter()
                .enumerate()
                .find(|(_, neighbour)| neighbour.id == message.src)
                .ok_or_eyre("Neighbour missing")?.0;

            state.neighbours[neighbour_index].broadcast_messages.extend(neighbours_messages.clone());
            state.broadcast_messages.extend(neighbours_messages);

            let reply = message.into_reply(Payload::SyncOk {
                broadcast_messages: state.broadcast_messages.difference(&state.neighbours[neighbour_index].broadcast_messages)
                    .copied()
                    .collect()
            });

            Some(reply)
        },
        Payload::SyncOk { broadcast_messages } => {
            let mut state = node.state.write().unwrap();

            state.neighbours.iter_mut()
                .find(|neighbour| neighbour.id == message.src)
                .ok_or_eyre("Neighbour missing")?
                .broadcast_messages
                .extend(broadcast_messages.clone());
            state.broadcast_messages.extend(broadcast_messages);

            None
        },
        Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {
            None
        },
    })
}

async fn sync_with_neighbours(node: Node<State, Payload>) -> Result<()> {
    let messages = {
        let mut state = node.state.write().unwrap();
        let broadcast_messages = state.broadcast_messages.clone();

        state.neighbours.iter_mut()
            .filter(|neighbour| neighbour.id < node.node_id)
            .map(|neighbour| {
                let messages_to_send = broadcast_messages.difference(&neighbour.broadcast_messages)
                    .copied()
                    .collect::<HashSet<_>>();

                neighbour.broadcast_messages.extend(messages_to_send.clone());

                (neighbour.id.clone(), messages_to_send)
            })
            .collect::<Vec<_>>()
    };

    for (dest, messages_to_send) in messages {
        node.send_new_message(dest, Payload::Sync {
            broadcast_messages: messages_to_send,
        }).await?;
    }

    Ok(())
}

#[derive(Default)]
struct State {
    neighbours: Vec<NeighbourState>,
    broadcast_messages: HashSet<i32>,
}

struct NeighbourState {
    id: NodeId,
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
        broadcast_messages: HashSet<i32>,
    },
    SyncOk {
        broadcast_messages: HashSet<i32>,
    },
}
