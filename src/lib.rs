use std::fmt::Debug;
use std::future::Future;
use std::io::{BufRead, Write};
use std::io;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use color_eyre::eyre::{eyre, OptionExt, Result};
use color_eyre::Report;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use crate::sequential_kv_store::{SEQUENTIAL_KV_STORE_ID, SequentialKVStore, SequentialKVStorePayload};

mod sequential_kv_store;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<P> {
    pub src: String,
    pub dest: String,
    pub body: MessageBody<P>,
}

impl<P> Message<P> {
    pub fn into_reply<P2>(self, payload: P2) -> Message<P2> {
        Message {
            src: self.dest,
            dest: self.src,
            body: MessageBody {
                message_id: self.body.in_reply_to.map(|id| id + 1),
                in_reply_to: self.body.message_id,
                payload,
            }
        }
    }

    pub fn take_payload(self) -> (Message<()>, P) {
        (
            Message {
                src: self.src,
                dest: self.dest,
                body: MessageBody {
                    message_id: self.body.message_id,
                    in_reply_to: self.body.in_reply_to,
                    payload: (),
                },
            },
            self.body.payload,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBody<P> {
    #[serde(rename = "msg_id", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<i32>,
    #[serde(flatten)]
    pub payload: P,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename = "init")]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename = "init_ok")]
pub struct InitOk {
    #[serde(skip)]
    pub _marker: PhantomData<bool>,
}

pub type NodeId = String;
pub type MessageReply<P> = Result<Option<Message<P>>>;
type MessageHandler<S, P> = Box<dyn Fn(Node<S, P>, Message<P>) -> BoxFuture<'static, MessageReply<P>> + Send + Sync>;
type TaskHandler<S, P> = Box<dyn Fn(Node<S, P>) -> BoxFuture<'static, Result<()>> + Send + Sync>;

pub struct Task<S, P> {
    handler: TaskHandler<S, P>,
    period: Duration,
}

pub struct NodeInner<S, P> {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
    pub state: RwLock<S>,
    pub seq_kv: SequentialKVStore,
    handler: MessageHandler<S, P>,
    message_channel_tx: Sender<String>,
}

pub struct Node<S, P> {
    inner: Arc<NodeInner<S, P>>,
}

impl<S, P> Clone for Node<S, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S, P> Deref for Node<S, P> {
    type Target = Arc<NodeInner<S, P>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S, P> Node<S, P>
where
    S: Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    async fn serve(
        self,
        mut message_channel_rx: Receiver<String>,
        tasks: Vec<Task<S, P>>,
    ) -> Result<()> {
        let mut set = JoinSet::new();

        set.spawn(async move {
            while let Some(message) = message_channel_rx.recv().await {
                let mut stdout = io::stdout().lock();

                stdout.write_all(message.as_bytes())?;
                stdout.write_all(b"\n")?;
            }

            Ok::<_, Report>(())
        });

        for task in tasks {
            let node = self.clone();
            set.spawn(async move {
                loop {
                    tokio::time::sleep(task.period).await;
                    (task.handler)(node.clone()).await?;
                }

                #[allow(unreachable_code)]
                Ok::<_, Report>(())
            });
        }

        let stdin = io::stdin();
        let mut line = String::new();

        while let Ok(_) = stdin.read_line(&mut line) {
            let value = serde_json::from_str::<serde_json::Value>(&line)?;
            line.clear();

            if value.get("src").ok_or_eyre("Missing dest field")?.as_str().ok_or_eyre("src field is not a string")? == SEQUENTIAL_KV_STORE_ID {
                let seq_kv_reply = serde_json::from_value::<Message<SequentialKVStorePayload>>(value)?;
                self.seq_kv.handle_reply(seq_kv_reply).await?;

                continue;
            }

            let message = serde_json::from_value::<Message<P>>(value)?;
            let node = self.clone();

            set.spawn(async move {
                let reply = (node.handler)(node.clone(), message).await?;

                if let Some(reply) = reply {
                    let reply = serde_json::to_string(&reply)?;

                    node.message_channel_tx.send(reply)
                        .await
                        .map_err(|_| eyre!("Failed to send message via message_channel"))?;
                }

                Ok::<_, Report>(())
            });
        }

        while let Some(res) = set.join_next().await {
            res??;
        }

        Ok(())
    }

    pub async fn send_new_message(&self, dest: String, payload: P) -> Result<()> {
        let message = Message {
            src: self.node_id.clone(),
            dest,
            body: MessageBody {
                message_id: None,
                in_reply_to: None,
                payload,
            },
        };

        let message = serde_json::to_string(&message)?;

        self.message_channel_tx.send(message)
            .await
            .map_err(|_| eyre!("Failed to send message via message_channel"))?;

        Ok(())
    }
}

pub struct NodeServer<S, P> {
    state: S,
    handler: MessageHandler<S, P>,
    tasks: Vec<Task<S, P>>,
}

impl<S, P> NodeServer<S, P>
where
    S: Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    pub fn new<H, Fut>(state: S, handler: H) -> Self
    where
        H: Fn(Node<S, P>, Message<P>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = MessageReply<P>> + Send + 'static,
    {
        Self {
            state,
            handler: Box::new(move |node, message| Box::pin(handler(node, message))),
            tasks: Vec::new(),
        }
    }

    pub fn add_task<H, Fut>(mut self, task: H, period: Duration) -> Self
    where
        H: Fn(Node<S, P>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.tasks.push(Task {
            handler: Box::new(move |node| Box::pin(task(node))),
            period,
        });
        self
    }

    pub async fn serve(self) -> Result<()> {
        color_eyre::install()?;

        let payload = {
            let mut stdin = io::stdin().lock();
            let mut stdout = io::stdout().lock();

            let mut line = String::new();
            stdin.read_line(&mut line)?;

            let init_message = serde_json::from_str::<Message<Init>>(&line)?;
            let (init_message, payload) = init_message.take_payload();

            let init_ok_message = init_message.into_reply(InitOk::default());
            serde_json::to_writer(&mut stdout, &init_ok_message)?;
            stdout.write_all(b"\n")?;

            payload
        };

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        let node = Node {
            inner: Arc::new(NodeInner {
                node_id: payload.node_id.clone(),
                node_ids: payload.node_ids,
                state: RwLock::new(self.state),
                seq_kv: SequentialKVStore::new(payload.node_id, tx.clone()),
                handler: self.handler,
                message_channel_tx: tx,
            }),
        };

        node.serve(rx, self.tasks).await
    }
}
