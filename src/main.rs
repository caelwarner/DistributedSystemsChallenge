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
        Payload::Echo { echo } => {
            node.send(&message.into_reply(Payload::EchoOk { echo }))?;
        },
        Payload::EchoOk { .. } => {
            // Do nothing
        },
    }

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}
