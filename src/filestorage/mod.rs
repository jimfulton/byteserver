/// filestorage2

#[macro_use]
mod util;

mod errors;
mod index;
mod lock;
mod pool;
mod records;
mod tid;
mod transaction;

use std::fs::OpenOptions;
use std::collections::VecDeque;
use std;

use self::errors::*;
use self::util::*;

static INDEX_SUFFIX: &'static str = ".index";

pub enum LoadBeforeResult {
    Loaded(Bytes, Tid, Option<Tid>),
    NoneBefore,
}

pub struct Conflict {
    oid: Oid,
    serials: (Tid, Tid),
    data: Bytes,
}

pub struct FileStorage {
    path: String,
    file: Mutex<File>,
    index: Mutex<index::Index>,
    readers: pool::FilePool<pool::ReadFileFactory>,
    tmps: pool::FilePool<pool::TmpFileFactory>,
    last_tid: Mutex<Tid>,
    locker: Mutex<lock::LockManager>,
    voted: Mutex<VecDeque<Voted>>,
    // TODO header: FileHeader,
}

pub struct Voted {
    id: Tid,
    pos: u64,
    tid: Tid,
    oids: Vec<Oid>,
    finished: Option<Box<Fn(Tid)>>,
}

impl FileStorage {

    fn new(path: String, file: File, index: index::Index, last_tid: Tid)
           -> io::Result<FileStorage> {
        Ok(FileStorage {
            readers: pool::FilePool::new(
                pool::ReadFileFactory { path: path.clone() }, 9),
            tmps: pool::FilePool::new(
                try!(pool::TmpFileFactory::base(path.clone() + ".tmp")), 22),
            path: path,
            file: Mutex::new(file),
            index: Mutex::new(index),
            last_tid: Mutex::new(last_tid),
            locker: Mutex::new(lock::LockManager::new()),
            voted: Mutex::new(VecDeque::new()),
        })
    }

    fn open(path: String) -> io::Result<FileStorage> {
        let mut file = try!(OpenOptions::new()
                            .read(true).write(true).create(true)
                            .open(&path));
        let size = try!(file.metadata()).len();
        if size == 0 {
            try!(records::FileHeader::new().write(&mut file));
            FileStorage::new(path, file, index::Index::new(), Z64)
        }
        else {
            records::FileHeader::read(&mut file); // TODO use header info
            let (index, last_tid) = try!(FileStorage::load_index(
                &(path.clone() + INDEX_SUFFIX), &mut file, size));
            FileStorage::new(path, file, index, last_tid)
        }
    }

    fn load_index(path: &str, mut file: &File, size: u64)
                  -> io::Result<(index::Index, Tid)> {
        let (mut index, segment_size, start, mut end) =
            try!(index::load_index(path));
            
        io_assert!(size >= segment_size, "Index bad segment length");
        try!(file.seek(io::SeekFrom::Start(records::HEADER_SIZE + 12)));
        io_assert!(try!(read8(&mut file)) == start, "Index bad start");
        try!(file.seek(io::SeekFrom::Start(segment_size - 8)));
        io_assert!(try!(read8(&mut file)) == end, "Index bad start");
        if segment_size < size {
            // Read newer records into index
            let mut reader = io::BufReader::new(try!(file.try_clone()));
            loop {
                let marker = try!(read4(&mut reader));
                match &marker {
                    m if m == TRANSACTION_MARKER => {
                        let header = try!(
                            records::TransactionHeader::read(&mut reader));
                        header.update_index(&mut reader, &mut index);
                        assert!(header.id > end);
                        end = header.id;
                    },
                    m if m == PADDING_MARKER => {
                        let length = try!(reader.read_u64::<LittleEndian>());
                        try!(reader.seek(io::SeekFrom::Current(
                            length as i64 - 12)));
                    },
                    _ => {
                        io_assert!(false, "Bad record marker");
                    }
                }
            }
        }
        Ok((index, end))
    }

    fn new_tid(&self) -> Tid {
        let mut last_tid = self.last_tid.lock().unwrap();
        *last_tid = tid::later_than(tid::now_tid(), *last_tid);
        *last_tid
    }
    
    fn lookup_pos(&self, oid: &Oid) -> Option<u64> {
        let index = self.index.lock().unwrap();
        index.get(oid).map(| pos | *pos)
    }

    fn load_before(&self, oid: Oid, tid: &Tid) -> Result<LoadBeforeResult> {
        match self.lookup_pos(&oid) {
            Some(pos) => {
                let p = try!(self.readers.get()
                             .chain_err(|| "getting reader"));
                let mut file = p.borrow_mut();
                try!(file.seek(io::SeekFrom::Start(pos))
                             .chain_err(|| "seeling to object record"));
                let mut header = try!(records::DataHeader::read(&mut &*file)
                                      .chain_err(|| "Reading object header"));
                let mut next: Option<Tid> = None;
                while &header.tid >= tid {
                    if header.previous == 0 {
                        return Ok(LoadBeforeResult::NoneBefore);
                    }
                    next = Some(header.tid);
                    try!(file.seek(io::SeekFrom::Start(header.previous))
                         .chain_err(|| "seeking to previous"));
                    header = try!(records::DataHeader::read(&mut &*file)
                                  .chain_err(|| "reading previous header"));
                }
                Ok(LoadBeforeResult::Loaded(
                    try!(read_sized(&mut &*file, header.length as usize)
                         .chain_err(|| "Reading object data")),
                    header.tid, next))
            },
            None =>
                Err(ErrorKind::POSKeyError(oid).into()),
        }
    }

    fn lock(&self, transaction: &transaction::Transaction, locked: Box<Fn()>)
            -> Result<()> {
        let (tid, oids) = try!(transaction.lock_data());
        let mut locker = self.locker.lock().unwrap();
        locker.lock(tid, oids, locked);
        Ok(())
    }

    pub fn tpc_begin(&self, user: &[u8], desc: &[u8], ext: &[u8])
                 -> io::Result<transaction::Transaction> {
        Ok(try!(transaction::Transaction::begin(
                try!(self.tmps.get()),
                self.new_tid(), user, desc, ext)))
    }

    fn stage(&self, trans: &mut transaction::Transaction)
             -> Result<Vec<Conflict>> {

        // Check for conflicts
        let oid_serials = {
            let mut oid_serials: Vec<(Oid, Tid)> = vec![];
            for r in try!(trans.serials().chain_err(|| "transaction serials")) {
                oid_serials.push(try!(r.chain_err(|| "transaction serial")));
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
                .collect::<Vec<(Oid, Tid, Option<u64>)>>()
        };
        let mut conflicts: Vec<Conflict> = vec![];
        let p = try!(self.readers.get().chain_err(|| "getting reader"));
        let mut file = p.borrow_mut();
        for (oid, serial, posop) in oid_serial_pos {
            match posop {
                Some(pos) => {
                    try!(file.seek(io::SeekFrom::Start(pos+12))
                         .chain_err(|| "Seeking to serial"));
                    let committed = try!(read8(&mut *file)
                                         .chain_err(|| "Reading serial"));
                    if committed != serial {
                        let data = try!(trans.get_data(&oid));
                        conflicts.push(
                            Conflict { oid: oid, data: data,
                                       serials: (serial, committed)
                            });
                    }
                    try!(trans.set_previous(&oid, pos));
                },
                None => {
                    if serial != Z64 {
                        return Err(ErrorKind::POSKeyError(oid).into());
                    }
                }
            }
        }

        if conflicts.len() == 0 {
            try!(trans.pack().chain_err(|| "trans pack"));
            let mut voted = self.voted.lock().unwrap();
            let mut file = self.file.lock().unwrap();
            let tid = self.new_tid();
            let pos = try!(file.seek(io::SeekFrom::End(0))
                           .chain_err(|| "seek end"));
            let oids = try!(trans.stage(tid, &mut file, pos)
                            .chain_err(|| "trans stage"));
            voted.push_front(
                Voted { id: trans.id, pos: pos, tid: tid, oids: oids,
                        finished: None });
        }
            
        Ok(conflicts)
    }

    fn tpc_finish(&self, id: &Tid, finished: Box<Fn(Tid)>) -> Result<()> {
        let mut voted = self.voted.lock().unwrap();

        for v in voted.iter_mut() {
            if v.id == *id {
                v.finished = Some(finished);
                break;
            }
        }
        
        while voted.len() > 0 {
            {
                let ref mut v = voted[0];
                if let Some(ref finished) = v.finished {
                    let mut file = self.file.lock().unwrap();
                    try!(file.seek(io::SeekFrom::Start(v.pos))
                         .chain_err(|| "seeking tpc_finish"));
                    try!(file.write_all(TRANSACTION_MARKER)
                         .chain_err(|| "writing trans marker tpc_finish"));
                    self.locker.lock().unwrap().release(&v.id);
                    // TODO notify other clents, v.oids
                }
                else {
                    break;
                }
            }
            voted.pop_front();
        }
        Ok(())
    }
}

fn basic_update() {
    let fs = FileStorage::open(String::from("data.fs")).unwrap();

    let mut t = fs.tpc_begin(b"", b"", b"").unwrap();
    t.save(Z64, Z64, b"xxxx").unwrap();
    t.save(p64(1), Z64, b"yyyy").unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    fs.lock(&t, Box::new(move || tx.send(true).unwrap()));
    rx.recv().unwrap();
    t.locked().unwrap();
    let conflicts = fs.stage(&mut t).unwrap();
    assert_eq!(conflicts.len(), 0);
    fs.tpc_finish(&t.id, Box::new(
        | tid | println!("{:?}", tid))).unwrap();
}


                     


    
//     let mut t = try!(s.tpc_begin(b"", b"", b""));
//     t.save([0u8; 8], b"xxxx");
//     //s.tpc_vote(t);
//     //s.tpc_finish(t);
        

//     Ok(())
// }






















































    
