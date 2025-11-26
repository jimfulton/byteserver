/// filestorage2

use std::io::prelude::*;

use anyhow::{Context, Result};
use byteorder::{ByteOrder, BigEndian, ReadBytesExt};

use crate::errors;
use crate::index;
use crate::lock;
use crate::pool;
use crate::records;
use crate::tid;
use crate::transaction;

use crate::util;

const INDEX_SUFFIX: &'static str = ".index";
const TRANSACTION_MARKER: &'static [u8] = b"TTTT";

#[derive(Debug)]
pub enum LoadBeforeResult {
    Loaded(util::Bytes, util::Tid, Option<util::Tid>),
    NoneBefore,
    PosKeyError,
}

#[derive(Debug, PartialEq)]
pub struct Conflict {
    pub oid: util::Oid,
    pub serial: util::Tid,
    pub committed: util::Tid,
    pub data: util::Bytes,
}

pub struct FileStorage<C: Client> {
    path: String,
    voted: std::sync::Mutex<std::collections::VecDeque<Voted<C>>>,
    file: std::sync::Mutex<std::fs::File>,
    index: std::sync::Mutex<index::Index>,
    readers: pool::FilePool<pool::ReadFileFactory>,
    tmps: pool::FilePool<pool::TmpFileFactory>,
    last_tid: std::sync::Mutex<util::Tid>,
    committed_tid: std::sync::Mutex<util::Tid>,
    locker: std::sync::Mutex<lock::LockManager>,
    clients: std::sync::Mutex<Vec<C>>,
    last_oid: std::sync::Mutex<u64>,
    // TODO header: FileHeader,
}

pub struct Voted<C: Client> {
    id: util::Tid,
    pos: u64,
    tid: util::Tid,
    length: u64,
    index: index::Index,
    finished: Option<C>,
}

pub trait Client: PartialEq + Send + Clone + std::fmt::Debug {
    fn finished(&self, tid: &util::Tid, len: u64, size: u64) -> Result<()>;
    fn invalidate(&self, tid: &util::Tid, oids: &Vec<util::Oid>) -> Result<()>;
    fn close(&self);
}

impl<C: Client> FileStorage<C> {

    fn new(path: String, file: std::fs::File, index: index::Index,
           last_tid: util::Tid, last_oid: util::Oid)
           -> std::io::Result<FileStorage<C>> {
        let last_oid = BigEndian::read_u64(&last_oid);
        Ok(FileStorage {
            readers: pool::FilePool::new(
                pool::ReadFileFactory { path: path.clone() }, 9),
            tmps: pool::FilePool::new(
                pool::TmpFileFactory::base(path.clone() + ".tmp")?,
                22),
            path: path,
            file: std::sync::Mutex::new(file),
            index: std::sync::Mutex::new(index),
            committed_tid: std::sync::Mutex::new(last_tid),
            last_tid: std::sync::Mutex::new(last_tid),
            locker: std::sync::Mutex::new(lock::LockManager::new()),
            voted: std::sync::Mutex::new(std::collections::VecDeque::new()),
            clients: std::sync::Mutex::new(Vec::new()),
            last_oid: std::sync::Mutex::new(last_oid),
        })
    }

    pub fn open(path: String) -> std::io::Result<FileStorage<C>> {
        let mut file =
            std::fs::OpenOptions::new()
            .read(true).write(true).create(true)
            .open(&path)?;
        let size = file.metadata()?.len();
        if size == 0 {
            records::FileHeader::new().write(&mut file)?;
            FileStorage::new(path, file, index::Index::new(), util::Z64, util::Z64)
        }
        else {
            records::FileHeader::read(&mut file); // TODO use header info
            let (index, last_tid, last_oid) = FileStorage::<C>::load_index(
                &(path.clone() + INDEX_SUFFIX), &mut file, size)?;
            FileStorage::new(path, file, index, last_tid, last_oid)
        }
    }

    pub fn add_client(&self, client: C) {
        self.clients.lock().unwrap().push(client);
    }

    pub fn remove_client(&self, client: C) {
        let mut clients = self.clients.lock().unwrap();
        clients.retain(| c | c != &client);
    }

    pub fn client_count(&self) -> usize {
        self.clients.lock().unwrap().len()
    }

    fn load_index(path: &str, mut file: &std::fs::File, size: u64)
                  -> std::io::Result<(index::Index, util::Tid, util::Oid)> {

        let (mut index, segment_size, mut end) =
            if std::path::Path::new(&path).exists() {
                let (index, segment_size, start, end) =
                    index::load_index(path)?;
                util::io_assert(size >= segment_size, "Index bad segment length")?;
                file.seek(std::io::SeekFrom::Start(records::HEADER_SIZE + 12))?;
                util::io_assert(util::read8(&mut file)? == start, "Index bad start")?;
                file.seek(std::io::SeekFrom::Start(segment_size - 8))?;
                util::io_assert(util::read8(&mut file)? == end, "Index bad end")?;
                (index, segment_size, end)
            }
            else {
                (index::Index::new(), records::HEADER_SIZE, util::Z64)
            };

        let mut last_oid = util::Z64;
        if segment_size < size {
            // Read newer records into index
            let mut reader = std::io::BufReader::new(file.try_clone()?);
            let mut pos = segment_size;
            util::seek(&mut reader, pos)?;
            while pos < size {
                let marker = util::read4(&mut reader)?;
                let length = match &marker {
                    m if m == TRANSACTION_MARKER => {
                        let header =
                            records::TransactionHeader::read(&mut reader)?;
                        last_oid = header.update_index(
                            &mut reader, &mut index, last_oid)?;
                        assert!(header.id > end);
                        end = header.id;
                        header.length
                    },
                    m if m == transaction::PADDING_MARKER => {
                        reader.read_u64::<BigEndian>()?
                    },
                    _ => {
                        util::io_assert(
                            false, &format!("Bad record marker {:?}", &marker))?;
                        0
                    }
                };
                pos += length;
                util::seek(&mut reader, pos - 8)?;
                assert_eq!(util::read_u64(&mut reader)?, length);
            }
        }
        Ok((index, end, last_oid))
    }

    fn new_tid(&self) -> util::Tid {
        let mut last_tid = self.last_tid.lock().unwrap();
        *last_tid = tid::later_than(tid::now_tid(), *last_tid);
        *last_tid
    }

    fn lookup_pos(&self, oid: &util::Oid) -> Option<u64> {
        let index = self.index.lock().unwrap();
        index.get(oid).map(| pos | *pos)
    }

    pub fn load_before(&self, oid: &util::Oid, tid: &util::Tid)
                       -> Result<LoadBeforeResult> {
        match self.lookup_pos(oid) {
            Some(pos) => {
                let p = self.readers.get().context("getting reader")?;
                let mut file = p.try_clone()?;
                file.seek(std::io::SeekFrom::Start(pos))
                    .context("seeking to object record")?;
                let mut header =
                    records::DataHeader::read(&mut &file)
                    .context("Reading object header")?;
                let mut next: Option<util::Tid> = None;
                while &header.tid >= tid {
                    if header.previous == 0 {
                        return Ok(LoadBeforeResult::NoneBefore);
                    }
                    next = Some(header.tid);
                    file.seek(std::io::SeekFrom::Start(header.previous))
                        .context("seeking to previous")?;
                    header =
                        records::DataHeader::read(&mut &file)
                        .context("reading previous header")?;
                }
                Ok(LoadBeforeResult::Loaded(
                    util::read_sized(&mut &file, header.length as usize)
                        .context("Reading object data")?,
                    header.tid, next))
            },
            None => Ok(LoadBeforeResult::PosKeyError),
        }
    }

    pub fn lock(&self,
                transaction: &transaction::Transaction,
                locked: Box<dyn Fn(util::Tid)>)
                -> Result<()> {
        let (tid, oids) = transaction.lock_data()?;
        let mut locker = self.locker.lock().unwrap();
        locker.lock(tid, oids, locked);
        Ok(())
    }

    pub fn new_oids(&self) -> Vec<util::Oid> {
        let mut last_oid = self.last_oid.lock().unwrap();
        let result: Vec<util::Oid> =
            (*last_oid + 1 .. *last_oid + 101).map(| oid | util::p64(oid)).collect();
        *last_oid += 100;
        result
    }

    pub fn tpc_begin(&self, user: &[u8], desc: &[u8], ext: &[u8])
                 -> std::io::Result<transaction::Transaction<'_>> {
        Ok(transaction::Transaction::begin(
                self.tmps.get()?,
                self.new_tid(), user, desc, ext)?)
    }

    pub fn stage(&self, trans: &mut transaction::Transaction)
             -> Result<Vec<Conflict>> {

        // Check for conflicts
        let oid_serials = {
            let mut oid_serials: Vec<(util::Oid, util::Tid)> = vec![];
            for r in trans.serials().context("transaction serials")? {
                oid_serials.push(r.context("transaction serial")?);
            };
            oid_serials
        };
        let oid_serial_pos = {
            let index = self.index.lock().unwrap();
            oid_serials.iter().map(
                | t | {
                    let (oid, serial) = *t;
                    (oid, serial, index.get(&oid).map(| r | r.clone()))
                })
                .collect::<Vec<(util::Oid, util::Tid, Option<u64>)>>()
        };
        let mut conflicts: Vec<Conflict> = vec![];
        let p = self.readers.get().context("getting reader")?;
        let mut file = p.try_clone()?;
        for (oid, serial, posop) in oid_serial_pos {
            match posop {
                Some(pos) => {
                    file.seek(std::io::SeekFrom::Start(pos+12))
                        .context("Seeking to serial")?;
                    let committed =
                        util::read8(&mut file).context("Reading serial")?;
                    if committed != serial {
                        let data = trans.get_data(&oid)?;
                        conflicts.push(
                            Conflict { oid: oid, data: data,
                                       serial: serial, committed: committed }
                            );
                    }
                    trans.set_previous(&oid, pos)?;
                },
                None => {
                    if serial != util::Z64 {
                        return Err(errors::POSError::Key(oid))?;
                    }
                }
            }
        }

        if conflicts.len() == 0 {
            trans.pack().context("trans pack")?;
            let mut voted = self.voted.lock().unwrap();
            let mut file = self.file.lock().unwrap();
            let tid = self.new_tid();
            let pos = file.seek(std::io::SeekFrom::End(0)).context("seek end")?;
            let (index, length) =
                trans.stage(tid, &mut file).context("trans stage")?;
            voted.push_back(
                Voted { id: trans.id, pos: pos, tid: tid, index: index,
                        finished: None, length: length });
        }
        else {
            trans.unlocked()?;
            self.locker.lock().unwrap().release(&trans.id);
        }

        Ok(conflicts)
    }

    pub fn tpc_finish(&self, id: &util::Tid, finished: C) -> Result<()> {
        let mut voted = self.voted.lock().unwrap();

        for v in voted.iter_mut() {
            if v.id == *id {
                v.finished = Some(finished);

                // Update the transaction maker right away, so if we
                // restart, the transaction will be there.  We don't
                // update the index and notify clients until earlier
                // voted transactions have finished.
                let mut file = self.file.lock().unwrap();
                file.seek(std::io::SeekFrom::Start(v.pos))
                    .context("seeking tpc_finish")?;
                file.write_all(TRANSACTION_MARKER)
                    .context("writing trans marker tpc_finish")?;
                file.sync_all().context("fsync")?;
                break;
            }
        }
        self.handle_finished_at_voted_head(voted);
        Ok(())
    }


    fn handle_finished_at_voted_head(
        &self,
        mut voted: std::sync::MutexGuard<std::collections::VecDeque<Voted<C>>>) {

        while voted.len() > 0 {
            {
                let ref mut v = voted.front().unwrap();
                if let Some(ref finished) = v.finished {
                    let len = {
                        let mut index = self.index.lock().unwrap();
                        for (k, pos) in v.index.iter() {
                            index.insert(k.clone(), *pos + v.pos);
                        };
                        index.len() as u64
                    };

                    let oids: Vec<util::Oid> = v.index.keys()
                        .map(| oid | oid.clone())
                        .collect();
                    *self.committed_tid.lock().unwrap() = v.tid;
                    let mut clients = self.clients.lock().unwrap();
                    let mut clients_to_remove: Vec<C> = vec![];

                    for client in clients.iter() {
                        if client != finished {
                            if client.invalidate(&v.tid, &oids).is_err() {
                                clients_to_remove.push((*client).clone());
                            }
                        }
                    }
                    if finished.finished(&v.tid, len, v.pos + v.length)
                        .is_err() {
                            clients_to_remove.push(finished.clone());
                        };
                    clients.retain(| c | ! clients_to_remove.contains(&c));
                    self.locker.lock().unwrap().release(&v.id);
                }
                else {
                    break;
                }
            }
            voted.pop_front();
        }
    }


    pub fn tpc_abort(&self, id: &util::Tid) {
        let mut voted = self.voted.lock().unwrap();
        let l = voted.len();
        voted.retain(
            | v | {
                if &v.id == id {
                    self.locker.lock().unwrap().release(id);
                    false
                }
                else {
                    true
                }
            }
        );
        if voted.len() == l {
            // May still need to unlock
            self.locker.lock().unwrap().release(id);
        }
        self.handle_finished_at_voted_head(voted);
    }

    pub fn last_transaction(&self) -> util::Tid {
        self.committed_tid.lock().unwrap().clone()
    }
}

// TODO save index on drop.
// impl std::ops::Drop for FileStorage {
//     fn drop(&mut self) {
//         let mut file = self.file.lock.unwrap();
//         let index = self.index.lock().unwrap();
//         let size = file.seek(std::io::SeekFrom::End(0));
//         let
//         index::save_index(&self.index, &(path.clone() + INDEX_SUFFIX))
//     }

// }

unsafe impl<C: Client> std::marker::Send for FileStorage<C> {}
unsafe impl<C: Client> std::marker::Sync for FileStorage<C> {}

pub mod testing {

    use super::*;
    
    pub const MAXTID: &'static util::Tid = b"\x7f\xff\xff\xff\xff\xff\xff\xff";

    #[derive(Debug, PartialEq, Clone)]
    struct NullClient;

    impl Client for NullClient {
        fn finished(&self, tid: &util::Tid, len: u64, size: u64) -> Result<()> {
            Ok(())
        }
        fn invalidate(&self, tid: &util::Tid, oids: &Vec<util::Oid>) -> Result<()> {
            Ok(())
        }
        fn close(&self) {}
    }

    pub fn make_sample(path: &String, transactions: Vec<Vec<(util::Oid, &[u8])>>)
                       -> Result<()> {
        // Create a storage with some initial data
        let fs: FileStorage<NullClient> =
            FileStorage::open(path.clone()).context("open fs")?;
        add_data(&fs, &NullClient, transactions)
    }

    pub fn add_data<C: Client>(fs: &FileStorage<C>,
                               client: &C,
                               transactions: Vec<Vec<(util::Oid, &[u8])>>)
                               -> Result<()> {
        
        let mut index = std::collections::BTreeMap::<util::Oid, util::Tid>::new();
        for saves in transactions {
            for &(oid, v) in saves.iter() {
                if let LoadBeforeResult::Loaded(_, tid, _) =
                    fs.load_before(&oid, MAXTID)? {
                    index.insert(oid.clone(), tid);
                }
            }
            let mut trans = fs.tpc_begin(b"", b"", b"").context("begin")?;
            for &(oid, v) in saves.iter() {
                let serial = index.get(&oid).or(Some(&util::Z64)).unwrap().clone();
                trans.save(oid, serial, v).context("sample data")?;
            }
            fs.lock(&trans, Box::new(| _ | ()))?;
            trans.locked()?;
            assert_eq!(fs.stage(&mut trans)?.len(), 0);
            fs.tpc_finish(&trans.id, client.clone())?;
            let tid = fs.last_transaction();
        }
        Ok(())
    }
}
