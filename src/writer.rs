use std;
use std::collections::{BTreeMap, HashMap};

use serde;

use storage;
use transaction;
use errors::*;
use util::*;
use msg::*;

macro_rules! respond {
    ($writer: expr, $id: expr, $data: expr) => (
        try!($writer.write_all(&response!($id, $data))
             .chain_err(|| "send response"))
    )
}

macro_rules! error {
    ($writer: expr, $id: expr, $data: expr) => (
        try!($writer.write_all(&error_response!($id, $data))
             .chain_err(|| "send error response"))
    )
}

macro_rules! async {
    ($writer: expr, $method: expr, $args: expr) => (
        try!($writer.write_all(&message!(0, true, $method, ($args)))
             .chain_err(|| "send async"))
    )
}

#[derive(Debug, Clone)]
pub struct Client {
    name: String,
    send: std::sync::mpsc::Sender<Zeo>,
    request_id: i64,
}

impl Client {
    pub fn new(name: String, send: std::sync::mpsc::Sender<Zeo>)
           -> Client {
        Client {name: name, send: send, request_id: 0}
    }
}

impl PartialEq for Client {
    fn eq(&self, other: &Client) -> bool {
        self.name == other.name
    }
}

impl storage::Client for Client {
    fn finished(&self, tid: &Tid, len: u64, size: u64) -> Result<()>  {
        self.send.send(
            Zeo::Finished(self.request_id, tid.clone(), len, size)
        ).chain_err(|| "send finished")
    }
    fn invalidate(&self, tid: &Tid, oids: &Vec<Oid>) -> Result<()>  {
        self.send.send(Zeo::Invalidate(
            tid.clone(), oids.clone())).chain_err(|| "send invalidate")
    }
    fn close(&self) {}
}

struct TransactionsHolder<'store> {
    fs: Arc<storage::FileStorage<Client>>,
    transactions: HashMap<u64, transaction::Transaction<'store>>,
}

impl<'store> Drop for TransactionsHolder<'store> {
    fn drop(&mut self) {
        for trans in self.transactions.values() {
            self.fs.tpc_abort(&trans.id);
        } 
    }
}

pub fn writer<W: io::Write>(
    fs: Arc<storage::FileStorage<Client>>,
    mut writer: W,
    receiver: std::sync::mpsc::Receiver<Zeo>,
    client: Client)
    -> Result<()> {

    try!(writer.write_all(&size_vec(b"M5".to_vec()))
         .chain_err(|| "writing handshake"));

    let mut transaction_holder = TransactionsHolder {
        fs: fs.clone(),
        transactions: HashMap::new(),
    };

    let transactions = &mut transaction_holder.transactions;
    
    for zeo in receiver.iter() {
        match zeo {
            Zeo::Raw(bytes) => {
                try!(writer.write_all(&bytes).chain_err(|| "writing raw"))
            },
            Zeo::TpcBegin(txn, user, desc, ext) => {
                if ! transactions.contains_key(&txn) {
                    transactions.insert(
                        txn,
                        try!(fs.tpc_begin(&user, &desc, &ext)
                             .chain_err(|| "writer begin")));
                }
            },
            Zeo::Storea(oid, serial, data, txn) => {
                if let Some(mut trans) = transactions.get_mut(&txn) {
                    try!(trans.save(oid, serial, &data)
                         .chain_err(|| "writer save"));
                }
            },
            Zeo::Vote(id, txn) => {
                if let Some(trans) = transactions.get(&txn) {
                    let send = client.send.clone();
                    try!(fs.lock(trans, Box::new(
                        move | _ | send.send(Zeo::Locked(id, txn))
                            .or::<Result<()>>(Ok(()))
                            .unwrap()
                    )));
                }
                else {
                    error!(writer, id,
                           ("ZODB.PosException.StorageTransactionError",
                            "Invalid transaction"));
                };
            },
            Zeo::Locked(id, txn) => {
                if let Some(mut trans) = transactions.get_mut(&txn) {
                    try!(trans.locked());
                    let conflicts = try!(fs.stage(&mut trans));
                    let conflict_maps:
                    Vec<BTreeMap<String, serde::bytes::Bytes>> =
                        conflicts.iter()
                        .map(| c | {
                            let mut m: BTreeMap<String, serde::bytes::Bytes> =
                                BTreeMap::new();
                            m.insert("oid".to_string(), bytes(&c.oid)); 
                            m.insert("serial".to_string(), bytes(&c.serial)); 
                            m.insert("committed".to_string(),
                                     bytes(&c.committed)); 
                            m.insert("data".to_string(), bytes(&c.data)); 
                            m
                        })
                        .collect();
                    respond!(writer, id, conflict_maps);
                }
            },
            Zeo::TpcFinish(id, txn) => {
                if let Some(trans) = transactions.remove(&txn) {
                    let mut client = client.clone();
                    client.request_id = id;
                    try!(fs.tpc_finish(&trans.id, client));
                }
                else {
                    error!(writer, id,
                           ("ZODB.PosException.StorageTransactionError",
                            "Invalid transaction"));
                }
            },
            Zeo::Finished(id, tid, len, size) => {
                respond!(writer, id, bytes(&tid));
                let mut info: BTreeMap<String, u64> = BTreeMap::new();
                info.insert("length".to_string(), len);
                info.insert("size".to_string(), size);
                async!(writer, "info", (info,));
            },
            Zeo::Invalidate(tid, oids) => {
                let oids: Vec<serde::bytes::Bytes> =
                    oids.iter().map(| oid | bytes(oid)).collect();
                async!(writer, "invalidateTransaction", (bytes(&tid), oids));
            },
            Zeo::TpcAbort(id, txn) => {
                if let Some(trans) = transactions.remove(&txn) {
                    fs.tpc_abort(&trans.id);
                }
                respond!(writer, id, NIL);

            },
            Zeo::End => break,
            _ => {}
        }
    }
    Ok(())
}
