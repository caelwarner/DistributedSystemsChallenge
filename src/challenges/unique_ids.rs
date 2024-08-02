use std::time::SystemTime;
use serde::{Deserialize, Serialize};
use color_eyre::Result;
use gossip_glomers::{Message, MessageReply, Node, NodeServer};

#[tokio::main]
async fn main() -> Result<()> {
    NodeServer::new((), message_handler)
        .serve()
        .await
}

async fn message_handler(node: Node<(), Payload>, message: Message<Payload>) -> MessageReply<Payload> {
    let (message, payload) = message.take_payload();

    Ok(match payload {
        Payload::Generate => {
            let time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;

            let flake_id = format!(
                "{}-{}",
                node.node_id,
                time.as_nanos(),
            );

            Some(message.into_reply(Payload::GenerateOk { id: flake_id }))
        },
        Payload::GenerateOk { .. } => {
            None
        },
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        id: String,
    },
}
