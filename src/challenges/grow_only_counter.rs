use color_eyre::Result;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use distributed_systems_challenge::{Message, MessageReply, Node, NodeServer};

#[tokio::main]
async fn main() -> Result<()> {
    NodeServer::new((), message_handler)
        .serve()
        .await
}

async fn message_handler(node: Node<(), Payload>, message: Message<Payload>) -> MessageReply<Payload> {
    let (message, payload) = message.take_payload();

    Ok(match payload {
        Payload::Add { delta } => {
            if delta != 0 {
                let value = node.seq_kv.read(node.node_id.clone()).await.unwrap_or(0);
                node.seq_kv.write(node.node_id.clone(), value + delta).await?;
            }

            Some(message.into_reply(Payload::AddOk))
        },
        Payload::Read => {
            let mut sum = 0;

            // Retry read multiple times to probabilistically ensure that we retrieve the most recent value
            for _ in 0..10 {
                let counters = node.node_ids.clone().into_iter()
                    .map(|id| node.seq_kv.read::<i32>(id));

                sum = join_all(counters).await.into_iter()
                    .map(|counter| counter.unwrap_or(0))
                    .sum();
            }

            Some(message.into_reply(Payload::ReadOk { value: sum }))
        },
        Payload::AddOk | Payload::ReadOk { .. } => {
            None
        },
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Add {
        delta: i32,
    },
    AddOk,
    Read,
    ReadOk {
        value: i32,
    },
}
