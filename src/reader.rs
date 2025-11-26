// Read side of server.

use anyhow::{anyhow, Context, Result};

use crate::storage;
use crate::writer;
use crate::msg;
use crate::msgmacros::*;

macro_rules! respond {
    ($sender: expr, $id: expr, $data: expr) => (
        $sender.send(msg::Zeo::Raw(response!($id, $data))).context("send response")?
    )
}

macro_rules! error {
    ($sender: expr, $id: expr, $data: expr) => (
        $sender
            .send(msg::Zeo::Raw(error_response!($id, $data)))
            .context("send error response")?
    )
}

pub fn reader<R: std::io::Read>(
    fs: std::sync::Arc<storage::FileStorage<writer::Client>>,
    reader: R,
    sender: std::sync::mpsc::Sender<msg::Zeo>)
    -> Result<()> {

    let mut it = msg::ZeoIter::new(reader);

    // handshake
    if it.next_vec()? != b"M5".to_vec() {
        return Err(anyhow!("Bad handshake"))?
    }

    // register(storage_id, read_only)
    loop {
        match it.next()? {
            msg::Zeo::Register(id, storage, read_only) => {
                if &storage != "1" {
                    error!(sender, id,
                           ("builtins.ValueError", ("Invalid storage",)))
                }
                respond!(sender, id, msg::bytes(&fs.last_transaction()));
                break;          // onward
            },
            msg::Zeo::End => {
                sender.send(msg::Zeo::End);
                return Ok(())
            },
            _ => return Err(anyhow!("bad method"))?
        }
    }

    // Main loop. We spend most of our time here.
    loop {
        let message = it.next()?;
        match message {
            msg::Zeo::LoadBefore(id, oid, before) => {
                use storage::LoadBeforeResult::*;
                match fs.load_before(&oid, &before)? {
                    Loaded(data, tid, Some(end)) => {
                        respond!(
                            sender, id,
                            (msg::bytes(&data), msg::bytes(&tid), msg::bytes(&end)));
                    },
                    Loaded(data, tid, None) => {
                        respond!(
                            sender, id,
                            (msg::bytes(&data), msg::bytes(&tid), msg::NIL));
                    },
                    NoneBefore => {
                        respond!(sender, id, msg::NIL);
                    },
                    PosKeyError => {
                        error!(sender, id,
                               ("ZODB.POSException.POSKeyError",
                                (msg::bytes(&oid),)));
                    },
                }
            },
            msg::Zeo::Ping(id) => {
                respond!(sender, id, msg::NIL);
            },
            msg::Zeo::NewOids(id) => {
                let oids = fs.new_oids();
                let oids: Vec<&[u8]> =
                    oids.iter().map(| oid | msg::bytes(oid)).collect();
                respond!(sender, id, oids)
            },
            msg::Zeo::GetInfo(id) => { // TODO, don't punt :)
                respond!(sender, id, std::collections::BTreeMap::<String, i64>::new())
            },
            msg::Zeo::TpcBegin(_, _, _, _) | msg::Zeo::Storea(_, _, _, _) |
            msg::Zeo::Vote(_, _) | msg::Zeo::TpcFinish(_, _) |  msg::Zeo::TpcAbort(_, _)
                =>
                sender
                .send(message)
                .context("send error")?, // Forward these
            msg::Zeo::End => {
                sender.send(msg::Zeo::End);
                return Ok(())
            },
            _ => return Err(anyhow!("bad method"))
        }            
    }
}
