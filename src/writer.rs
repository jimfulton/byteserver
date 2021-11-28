use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};

use crate::storage;
use crate::transaction;
use crate::util;
use crate::msg;
use crate::msgmacros::*;

macro_rules! respond {
    ($writer: expr, $id: expr, $data: expr) => (
        $writer.write_all(&response!($id, $data))
             .context("send response")?
    )
}

macro_rules! error {
    ($writer: expr, $id: expr, $data: expr) => (
        $writer.write_all(&error_response!($id, $data))
            .context("send error response")?
    )
}

macro_rules! async_ {
    ($writer: expr, $method: expr, $args: expr) => (
        $writer.write_all(&message!(0, $method, ($args)))
            .context("send async")?
    )
}

#[derive(Debug, Clone)]
pub struct Client {
    name: String,
    send: std::sync::mpsc::Sender<msg::Zeo>,
    request_id: i64,
}

impl Client {
    pub fn new(name: String, send: std::sync::mpsc::Sender<msg::Zeo>)
           -> Client {
        Client {name: name, send: send, request_id: 0}
    }
}

impl PartialEq for Client {
    fn eq(&self, other: &Client) -> bool {
        self.name == other.name
    }
}

impl crate::storage::Client for Client {
    fn finished(&self, tid: &util::Tid, len: u64, size: u64) -> Result<()>  {
        self.send.send(
            msg::Zeo::Finished(self.request_id, tid.clone(), len, size)
        ).context("send finished")
    }
    fn invalidate(&self, tid: &util::Tid, oids: &Vec<util::Oid>) -> Result<()>  {
        self.send.send(msg::Zeo::Invalidate(
            tid.clone(), oids.clone())).context("send invalidate")
    }
    fn close(&self) {}
}

struct TransactionsHolder<'store> {
    fs: std::sync::Arc<storage::FileStorage<Client>>,
    transactions: HashMap<u64, transaction::Transaction<'store>>,
}

impl<'store> Drop for TransactionsHolder<'store> {
    fn drop(&mut self) {
        for trans in self.transactions.values() {
            self.fs.tpc_abort(&trans.id);
        } 
    }
}

pub fn writer<W: std::io::Write>(
    fs: std::sync::Arc<storage::FileStorage<Client>>,
    mut writer: W,
    receiver: std::sync::mpsc::Receiver<msg::Zeo>,
    client: Client)
    -> Result<()> {

    writer.write_all(&msg::size_vec(b"M5".to_vec()))
        .context("writing handshake")?;

    let mut transaction_holder = TransactionsHolder {
        fs: fs.clone(),
        transactions: HashMap::new(),
    };

    let transactions = &mut transaction_holder.transactions;
    
    for zeo in receiver.iter() {
        match zeo {
            msg::Zeo::Raw(bytes) => {
                writer.write_all(&bytes).context("writing raw")?
            },
            msg::Zeo::TpcBegin(txn, user, desc, ext) => {
                if ! transactions.contains_key(&txn) {
                    transactions.insert(
                        txn,
                        fs.tpc_begin(&user, &desc, &ext)
                             .context("writer begin")?);
                }
            },
            msg::Zeo::Storea(oid, serial, data, txn) => {
                if let Some(trans) = transactions.get_mut(&txn) {
                    trans.save(oid, serial, &data)
                        .context("writer save")?;
                }
            },
            msg::Zeo::Vote(id, txn) => {
                if let Some(trans) = transactions.get(&txn) {
                    let send = client.send.clone();
                    fs.lock(trans, Box::new(
                        move | _ | send.send(msg::Zeo::Locked(id, txn))
                            .or::<Result<()>>(Ok(()))
                            .unwrap()
                    ))?;
                }
                else {
                    error!(writer, id,
                           ("ZODB.PosException.StorageTransactionError",
                            "Invalid transaction"));
                };
            },
            msg::Zeo::Locked(id, txn) => {
                if let Some(mut trans) = transactions.get_mut(&txn) {
                    trans.locked()?;
                    let conflicts = fs.stage(&mut trans)?;
                    let conflict_maps:
                    Vec<BTreeMap<String, serde::bytes::Bytes>> =
                        conflicts.iter()
                        .map(| c | {
                            let mut m: BTreeMap<String, serde::bytes::Bytes> =
                                BTreeMap::new();
                            m.insert("oid".to_string(), msg::bytes(&c.oid)); 
                            m.insert("serial".to_string(), msg::bytes(&c.serial)); 
                            m.insert("committed".to_string(),
                                     msg::bytes(&c.committed)); 
                            m.insert("data".to_string(), msg::bytes(&c.data)); 
                            m
                        })
                        .collect();
                    respond!(writer, id, conflict_maps);
                }
            },
            msg::Zeo::TpcFinish(id, txn) => {
                if let Some(trans) = transactions.remove(&txn) {
                    let mut client = client.clone();
                    client.request_id = id;
                    fs.tpc_finish(&trans.id, client)?;
                }
                else {
                    error!(writer, id,
                           ("ZODB.PosException.StorageTransactionError",
                            "Invalid transaction"));
                }
            },
            msg::Zeo::Finished(id, tid, len, size) => {
                respond!(writer, id, msg::bytes(&tid));
                let mut info: BTreeMap<String, u64> = BTreeMap::new();
                info.insert("length".to_string(), len);
                info.insert("size".to_string(), size);
                async_!(writer, "info", (info,));
            },
            msg::Zeo::Invalidate(tid, oids) => {
                let oids: Vec<serde::bytes::Bytes> =
                    oids.iter().map(| oid | msg::bytes(oid)).collect();
                async_!(writer, "invalidateTransaction", (msg::bytes(&tid), oids));
            },
            msg::Zeo::TpcAbort(id, txn) => {
                if let Some(trans) = transactions.remove(&txn) {
                    fs.tpc_abort(&trans.id);
                }
                respond!(writer, id, msg::NIL);

            },
            msg::Zeo::End => break,
            _ => {}
        }
    }
    Ok(())
}
