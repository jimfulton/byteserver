// Read side of server.
use std;
use std::collections::BTreeMap;
use serde;

use storage;
use writer;
use errors::*;
use util::*;
use msg::*;

macro_rules! respond {
    ($sender: expr, $id: expr, $data: expr) => (
        try!($sender.send(Zeo::Raw(response!($id, $data)))
             .chain_err(|| "send response"))
    )
}

macro_rules! error {
    ($sender: expr, $id: expr, $data: expr) => (
        try!($sender.send(Zeo::Raw(error_response!($id, $data)))
             .chain_err(|| "send error response"))
    )
}

pub fn reader<R: io::Read>(
    fs: Arc<storage::FileStorage<writer::Client>>,
    reader: R,
    sender: std::sync::mpsc::Sender<Zeo>)
    -> Result<()> {

    let mut it = ZeoIter::new(reader);

    // handshake
    if try!(it.next_vec()) != b"M5".to_vec() {
        return Err("Bad handshake".into())
    }

    // register(storage_id, read_only)
    loop {
        match try!(it.next()) {
            Zeo::Register(id, storage, read_only) => {
                if &storage != "1" {
                    error!(sender, id,
                           ("builtins.ValueError", ("Invalid storage",)))
                }
                respond!(sender, id, bytes(&fs.last_transaction()));
                break;          // onward
            },
            Zeo::End => {
                sender.send(Zeo::End);
                return Ok(())
            },
            _ => return Err("bad method".into())
        }
    }

    // Main loop. We spend most of our time here.
    loop {
        let message = try!(it.next());
        match message {
            Zeo::LoadBefore(id, oid, before) => {
                use storage::LoadBeforeResult::*;
                match try!(fs.load_before(&oid, &before)) {
                    Loaded(data, tid, Some(end)) => {
                        respond!(sender, id,
                                 (bytes(&data), bytes(&tid), bytes(&end)));
                    },
                    Loaded(data, tid, None) => {
                        respond!(sender, id, (bytes(&data), bytes(&tid), NIL));
                    },
                    NoneBefore => {
                        respond!(sender, id, NIL);
                    },
                    PosKeyError => {
                        error!(sender, id,
                               ("ZODB.POSException.POSKeyError",
                                (bytes(&oid),)));
                    },
                }
            },
            Zeo::Ping(id) => {
                respond!(sender, id, NIL);
            },
            Zeo::NewOids(id) => {
                let oids = fs.new_oids();
                let oids: Vec<serde::bytes::Bytes> =
                    oids.iter().map(| oid | bytes(oid)).collect();
                respond!(sender, id, oids)
            },
            Zeo::GetInfo(id) => { // TODO, don't punt :)
                respond!(sender, id, BTreeMap::<String, i64>::new())
            },
            Zeo::TpcBegin(_, _, _, _) | Zeo::Storea(_, _, _, _) |
            Zeo::Vote(_, _) | Zeo::TpcFinish(_, _) |  Zeo::TpcAbort(_, _)
                =>  try!(sender.send(message)
                         .chain_err(|| "send error")), // Forward these
            Zeo::End => {
                sender.send(Zeo::End);
                return Ok(())
            },
            _ => return Err("bad method".into())
        }            
    }
}
