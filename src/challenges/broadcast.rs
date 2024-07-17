use std::collections::HashMap;
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use gossip_glomers::{Message, Node, NodeId};

fn main() -> Result<()> {
    let node = Node::new(State::default(), message_handler)?;

    node.run()
}

fn message_handler(node: &mut Node<State, Payload>, message: Message<Payload>) -> Result<()> {
    let (message, payload) = message.take_payload();

    match payload {
        Payload::Broadcast { broadcast_message } => {
            node.state.broadcast_messages.push(broadcast_message);
            node.send(&message.into_reply(Payload::BroadcastOk))?;
        },
        Payload::Read => {
            node.send(&message.into_reply(Payload::ReadOk {
                broadcast_messages: node.state.broadcast_messages.clone(),
            }))?;
        },
        Payload::Topology { .. } => {
            node.send(&message.into_reply(Payload::TopologyOk))?;
        },
        Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {
            // Do nothing
        },
    }

    Ok(())
}

#[derive(Default)]
struct State {
    broadcast_messages: Vec<i32>,
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
        broadcast_messages: Vec<i32>,
    },
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    TopologyOk,
}
