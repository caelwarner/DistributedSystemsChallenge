use color_eyre::Result;
use std::fmt::Debug;
use std::marker::PhantomData;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Default)]
pub struct Node<S> {
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub state: S,
}
