use color_eyre::Result;
use std::io::{BufRead, Write};
use color_eyre::eyre::OptionExt;
use serde::{Deserialize, Serialize};
use gossip_glomers::{Init, InitOk, Message, Node};

fn main() -> Result<()> {
    let mut stdin = std::io::stdin().lock().lines();
    let mut stdout = std::io::stdout().lock();

    let line = stdin.next().ok_or_eyre("Missing init message")??;

    let init_message = serde_json::from_str::<Message<Init>>(&line)?;
    let (init_message, payload) = init_message.take_payload();
    let _node = Node {
        node_id: payload.node_id,
        node_ids: payload.node_ids,
        state: (),
    };


    let init_ok_message = init_message.into_reply(InitOk::default());
    serde_json::to_writer(&mut stdout, &init_ok_message)?;
    stdout.write_all(b"\n")?;

    for line in stdin {
        let line = line?;

        let message = serde_json::from_str::<Message<Payload>>(&line)?;
        let (message, payload) = message.take_payload();
        match payload {
            Payload::Echo { echo } => {
                serde_json::to_writer(&mut stdout, &message.into_reply(Payload::EchoOk { echo }))?;
                stdout.write_all(b"\n")?;
            },
            Payload::EchoOk { .. } => {},
        }
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
