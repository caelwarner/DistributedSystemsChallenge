use serde::{Deserialize, Serialize};
use color_eyre::Result;
use gossip_glomers::{Message, MessageReply, Node, NodeServer};

#[tokio::main]
async fn main() -> Result<()> {
    NodeServer::new((), message_handler)
        .serve()
        .await
}

async fn message_handler(_node: Node<(), Payload>, message: Message<Payload>) -> MessageReply<Payload> {
    let (message, payload) = message.take_payload();

    Ok(match payload {
        Payload::Echo { echo } => {
            Some(message.into_reply(Payload::EchoOk { echo }))
        },
        Payload::EchoOk { .. } => {
            None
        },
    })
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
