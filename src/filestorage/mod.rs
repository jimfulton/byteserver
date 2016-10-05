/// filestorage2

#[macro_use]
mod util;

mod errors;
mod index;
mod pool;
mod records;
mod tid;
mod transaction;

use std::fs::OpenOptions;
use std::sync::Mutex;

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
    file: File,
    index: index::Index,
    readers: pool::FilePool<pool::ReadFileFactory>,
    tmps: pool::FilePool<pool::TmpFileFactory>,
    last_tid: Mutex<Tid>,
    // TODO header: FileHeader,
}

impl FileStorage {

    fn new(path: String, file: File, index: index::Index, last_tid: Tid)
           -> io::Result<FileStorage> {
        Ok(FileStorage {
            readers: pool::FilePool::new(
                pool::ReadFileFactory { path: path.clone() }, 9),
            tmps: pool::FilePool::new(
                try!(pool::TmpFileFactory::base(path.clone() + ".tmp")), 22),
            path: path, file: file, index: index,
            last_tid: Mutex::new(last_tid),
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
        // TODO: index mutex
        self.index.get(oid).map(| pos | *pos)
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

    fn store(&mut self, oid: Oid, tid: Tid, serial: Tid, data: Bytes,
             transaction: &mut transaction::Transaction)
             -> Result<Option<Conflict>> {

        let mut previous = 0u64;
        match self.lookup_pos(&oid) {
            Some(pos) => {
                try!(self.file.seek(io::SeekFrom::Start(pos+12))
                     .chain_err(|| "Seeking to serial"));
                let committed = try!(read8(&mut self.file)
                                     .chain_err(|| "Reading serial"));
                if committed != serial {
                    return Ok(Some(Conflict { oid: oid, data: data,
                                              serials: (serial, committed) }));
                }
                previous = pos;
            },
            None => {
                if serial != Z64 {
                    return Err(ErrorKind::POSKeyError(oid).into());
                }
            }
        }
        transaction.write(&oid, &tid, previous, &data);
        Ok(None)
    }

    fn tpc_begin(&self, user: &[u8], desc: &[u8], ext: &[u8])
                 -> io::Result<transaction::Transaction> {
        transaction::Transaction::begin(
            try!(self.tmps.get()), self.new_tid(), user, desc, ext)
            
    }

    // fn tpc_vote(&mut self, &mut transaction::Transaction)
    //             -> io::Result<()> {
    //     for (oid, data) in transaction.saved() {
    //         try

    //     }

    // }

}

fn try_it() -> io::Result<()> {
    let s = try!(FileStorage::open("data.fs".to_string()));
    let mut t = try!(s.tpc_begin(b"", b"", b""));
    t.save([0u8; 8], b"xxxx");
    //s.tpc_vote(t);
    //s.tpc_finish(t);
        

    Ok(())
}






















































    
