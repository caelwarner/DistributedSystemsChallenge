use std::time::SystemTime;
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use gossip_glomers::{Message, Node};

fn main() -> Result<()> {
    let node = Node::new((), message_handler)?;

    node.run()
}

fn message_handler(node: &mut Node<(), Payload>, message: Message<Payload>) -> Result<()> {
    let (message, payload) = message.take_payload();

    match payload {
        Payload::Generate => {
            let time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;

            let flake_id = format!(
                "{}-{}",
                node.node_id,
                time.as_nanos(),
            );

            node.send(&message.into_reply(Payload::GenerateOk { id: flake_id }))?;
        },
        Payload::GenerateOk { .. } => {
            // Do nothing
        },
    }

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        id: String,
    },
}
