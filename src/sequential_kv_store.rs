use std::collections::HashMap;
use std::sync::Mutex;
use color_eyre::eyre::{eyre, OptionExt};
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::Sender;
use crate::{Message, MessageBody, NodeId};

pub const SEQUENTIAL_KV_STORE_ID: &str = "seq-kv";

pub type OneshotSender<T> = tokio::sync::oneshot::Sender<T>;

pub struct SequentialKVStore {
    node_id: NodeId,
    message_channel_tx: Sender<String>,
    state: Mutex<SequentialKVStoreState>,
}

struct SequentialKVStoreState {
    message_id: i32,
    reply_senders: HashMap<i32, OneshotSender<Message<SequentialKVStorePayload>>>,
}

impl SequentialKVStore {
    pub fn new(node_id: NodeId, tx: Sender<String>) -> Self {
        Self {
            node_id,
            message_channel_tx: tx,
            state: Mutex::new(SequentialKVStoreState {
                message_id: 0,
                reply_senders: HashMap::new(),
            }),
        }
    }

    pub(crate) async fn handle_reply(&self, reply: Message<SequentialKVStorePayload>) -> Result<()> {
        let tx = self.state.lock().unwrap().reply_senders
            .remove(&reply.body.in_reply_to.ok_or_eyre("Missing in_reply_to field")?)
            .ok_or_eyre("Missing reply sender")?;

        tx.send(reply).map_err(|_| eyre!("Failed to send {SEQUENTIAL_KV_STORE_ID} reply via oneshot channel"))?;
        Ok(())
    }

    pub async fn read<D: DeserializeOwned>(&self, key: String) -> Result<D> {
        match self.send_message(SequentialKVStorePayload::Read { key }).await? {
            SequentialKVStorePayload::ReadOk { value } => Ok(serde_json::from_str(&value)?),
            SequentialKVStorePayload::Error => Err(eyre!("Missing key in {SEQUENTIAL_KV_STORE_ID}")),
            _ => Err(eyre!("Wrong {SEQUENTIAL_KV_STORE_ID} reply type")),
        }
    }

    pub async fn write<S: Serialize>(&self, key: String, value: S) -> Result<()> {
        let value = serde_json::to_string(&value)?;

        match self.send_message(SequentialKVStorePayload::Write { key, value }).await? {
            SequentialKVStorePayload::WriteOk => Ok(()),
            _ => Err(eyre!("Wrong {SEQUENTIAL_KV_STORE_ID} reply type")),
        }
    }

    pub async fn cas<S: Serialize>(&self, key: String, from: S, to: S) -> Result<()> {
        let from = serde_json::to_string(&from)?;
        let to = serde_json::to_string(&to)?;

        match self.send_message(SequentialKVStorePayload::Cas { key, from, to, create_if_not_exists: true }).await? {
            SequentialKVStorePayload::CasOk => Ok(()),
            _ => Err(eyre!("Wrong {SEQUENTIAL_KV_STORE_ID} reply type")),
        }
    }

    async fn send_message(&self, payload: SequentialKVStorePayload) -> Result<SequentialKVStorePayload> {
        let (message, rx) = {
            let mut state = self.state.lock().unwrap();
            let message_id = state.message_id;

            let message = Message {
                src: self.node_id.clone(),
                dest: SEQUENTIAL_KV_STORE_ID.to_string(),
                body: MessageBody {
                    message_id: Some(message_id),
                    in_reply_to: None,
                    payload,
                },
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            state.reply_senders.insert(message_id, tx);
            state.message_id += 1;

            (message, rx)
        };

        self.message_channel_tx.send(serde_json::to_string(&message)?)
            .await
            .map_err(|_| eyre!("Failed to send {SEQUENTIAL_KV_STORE_ID} message via message_channel"))?;

        let reply = rx.await?;
        Ok(reply.body.payload)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum SequentialKVStorePayload {
    Read {
        key: String,
    },
    ReadOk {
        value: String,
    },
    Write {
        key: String,
        value: String,
    },
    WriteOk,
    Cas {
        key: String,
        from: String,
        to: String,
        create_if_not_exists: bool,
    },
    CasOk,
    Error,
}