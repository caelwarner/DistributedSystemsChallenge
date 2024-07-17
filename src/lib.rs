use color_eyre::Result;
use std::fmt::Debug;
use std::io::{BufRead, StdoutLock, Write};
use std::marker::PhantomData;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

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

pub type MessageHandler<S, P> = fn(&mut Node<'_, S, P>, Message<P>) -> Result<()>;

#[derive(Debug)]
pub struct Node<'a, S, P> {
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub state: S,
    handler: MessageHandler<S, P>,
    stdout: StdoutLock<'a>,
}

impl<S, P> Node<'_, S, P>
    where P: Serialize + DeserializeOwned,
{
    pub fn new(state: S, handler: MessageHandler<S, P>) -> Result<Self>
    {
        let mut stdin = std::io::stdin().lock();
        let mut stdout = std::io::stdout().lock();

        let mut line = String::new();
        stdin.read_line(&mut line)?;

        let init_message = serde_json::from_str::<Message<Init>>(&line)?;
        let (init_message, payload) = init_message.take_payload();

        let init_ok_message = init_message.into_reply(InitOk::default());
        serde_json::to_writer(&mut stdout, &init_ok_message)?;
        stdout.write_all(b"\n")?;

        Ok(Self {
            node_id: payload.node_id,
            node_ids: payload.node_ids,
            state,
            handler,
            stdout,
        })
    }

    pub fn run(mut self) -> Result<()> {
        for line in std::io::stdin().lock().lines() {
            let line = line?;

            let message = serde_json::from_str::<Message<P>>(&line)?;
            (self.handler)(&mut self, message)?;
        }

        Ok(())
    }

    pub fn send(&mut self, message: &Message<P>) -> Result<()> {
        serde_json::to_writer(&mut self.stdout, &message)?;
        self.stdout.write_all(b"\n")?;

        Ok(())
    }
}
