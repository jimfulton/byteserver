use std;

use super::errors::*;
use super::util::*;
use super::index::Index;
use super::pool;
use super::records;

static PADDING16: [u8; 16] = [0u8; 16]; 

pub type TransactionM<'a> = Arc<Mutex<Transaction<'a>>>;

// states: saving, locking, voted, finished

// struct TransactionData {
//     filep: pool::TmpFilePointer<'store>,
//     writer: io::BufWriter<File>,
//     length: u64,
//     header_length: u64,
    
// }

// pub enum TransactionState {
//     Saving {
//         filep: pool::TmpFilePointer<'store>,
//         writer: io::BufWriter<File>,
//     },
//     Locking {
//         filep: pool::TmpFilePointer<'store>,
//         writer: io::BufWriter<File>,
//     },
//     Finishing {
//         pos: u64,
//         callback: Box<Fn>,
//     }
    

//         (pool::TmpFilePointer<'store>, io::BufWriter<File>),

// }

pub struct Transaction<'store> {
    pub id: Tid, 
    pub tid: Tid, // committed tid after vote
    file: pool::TmpFilePointer<'store>,
    writer: io::BufWriter<File>,
    length: u64,
    header_length: u64,
    index: Index,
    needs_to_be_packed: bool,
    locked: bool,
    pub finished: Option<Box<Fn()>>,
    pub commit_pos: u64,
}

impl<'store, 't> Transaction<'store> {

    pub fn begin(file: pool::PooledFilePointer<'store, pool::TmpFileFactory>,
                 id: Tid, user: &[u8], desc: &[u8], ext: &[u8])
                 -> io::Result<Transaction<'store>> {
        let mut writer = io::BufWriter::new(try!(file.borrow().try_clone()));
        try!(writer.write_all(PADDING_MARKER));
        try!(writer.write_all(&PADDING16)); // tlen, tid
        try!(writer.write_u32::<LittleEndian>(0 as u32)); // count
        try!(writer.write_u16::<LittleEndian>(user.len() as u16));
        try!(writer.write_u16::<LittleEndian>(desc.len() as u16));
        try!(writer.write_u32::<LittleEndian>(ext.len() as u32));
        if user.len() > 0 { try!(writer.write_all(user)) }
        if desc.len() > 0 { try!(writer.write_all(desc)) }
        if  ext.len() > 0 { try!(writer.write_all(ext)) }
        let length = 4u64 + records::TRANSACTION_HEADER_LENGTH +
            user.len() as u64 + desc.len() as u64 + ext.len() as u64;
        Ok(Transaction {
            file: file, writer: writer, index: Index::new(),
            id: id, length: length, header_length: length, locked: false,
            tid: Z64, needs_to_be_packed: false, finished: None,
            commit_pos: 0,
        })
    }

    pub fn save(&mut self, oid: Oid, serial: Tid, data: &[u8])
                -> io::Result<()> {
        // Save data in the first phase of 2-phase commit.
        try!(self.writer.write_u32::<LittleEndian>(data.len() as u32));
        try!(self.writer.write_all(&oid));
        try!(self.writer.write_all(&serial));
        try!(self.writer.write_all(&PADDING16)); // previous and offset
        if data.len() > 0 { try!(self.writer.write_all(data)) }
        if self.index.insert(oid, self.length).is_some() {
            // There was an earlier save for this oid.  We'll want to
            // pacl the data before committing.
            self.needs_to_be_packed = true;
        };
        self.length += records::DATA_HEADER_SIZE + data.len() as u64;
        Ok(())
    }

    pub fn serials(&'t self) -> io::Result<TransactionSerialIterator<'t>> {
        TransactionSerialIterator::new(
            try!(self.file.borrow().try_clone()),
            &self.index, self.length, self.header_length)
    }

    pub fn lock(&mut self) -> Result<(Tid, Vec<Oid>)> {
        try!(self.writer.flush().chain_err(|| "trans writer flush"));
        let mut oids =
            self.index.keys().map(| r | r.clone()).collect::<Vec<Oid>>();
        oids.reverse();
        self.locked = true;
        Ok((self.id, oids))
    }

    pub fn get_data(&mut self, oid: &Oid) -> Result<Bytes> {
        let pos = try!(
            self.index.get(oid).ok_or(Error::from("trans index error")));
        let mut file = self.file.borrow_mut();
        try!(file.seek(io::SeekFrom::Start(*pos)).chain_err(|| "trans seek"));
        let dlen = try!(file.read_u32::<LittleEndian>()
                        .chain_err(|| "trans read dlen"));
        let data = if dlen > 0 {
            try!(file.seek(io::SeekFrom::Start(pos + records::DATA_HEADER_SIZE))
                 .chain_err(|| "trans seek data"));
            try!(read_sized(&mut *file, dlen as usize)
                 .chain_err(|| "trans read data"))
        }
        else {
            vec![0u8; 0]
        };
        Ok(data)
    }

    pub fn set_previous(&mut self, oid: &Oid, previous: u64) -> Result<()> {
        let pos = try!(
            self.index.get(oid).ok_or(Error::from("trans index error")));
        let mut file = self.file.borrow_mut();
        try!(file.seek(io::SeekFrom::Start(pos + records::DATA_PREVIOUS_OFFSET))
             .chain_err(|| "trans seek prev"));
        try!(file.write_u64::<LittleEndian>(previous)
             .chain_err(|| "trans write previous"));
        Ok(())
    }

    pub fn pack(&mut self) -> io::Result<()> {
        // If necessary, pack out records that were overwritten.
        // Also write length into header.
        let mut file = self.file.borrow_mut();

        if self.needs_to_be_packed {
            let mut rpos = self.header_length;
            let mut wpos = self.header_length;

            let mut buf = [0u8; 12];
            while rpos < self.length {
                try!(file.seek(io::SeekFrom::Start(rpos)));
                try!(file.read_exact(&mut buf));
                let dlen = LittleEndian::read_u32(&buf) as u64;
                let oid = try!(read8(&mut &buf[4..]));
                let oid_pos =
                    try!(self.index.get(&oid)
                         .ok_or(io_error("trans index get")));
                if *oid_pos == rpos {
                    // We want this one
                    if rpos != wpos {
                        // We need to move it.
                        let rest =
                            try!(read_sized(
                                &mut *file,
                                dlen as usize +
                                    records::DATA_HEADER_SIZE as usize - 12));
                        try!(file.seek(io::SeekFrom::Start(wpos)));
                        try!(file.write_all(&buf));
                        try!(file.write_all(&rest));
                        wpos += dlen + records::DATA_HEADER_SIZE;
                    }
                }
                rpos += dlen + records::DATA_HEADER_SIZE;
            }
            self.length = wpos;
            try!(file.set_len(wpos));
        }

        // Update header w length
        let full_length = self.length + 8;
        try!(file.seek(io::SeekFrom::Start(self.length)));
        try!(file.write_u64::<LittleEndian>(full_length));
        try!(file.seek(io::SeekFrom::Start(4)));
        try!(file.write_u64::<LittleEndian>(full_length));

        Ok(())
    }

    pub fn save_tid(&mut self, tid: Tid) -> io::Result<()> {
        try!(self.writer.seek(io::SeekFrom::Start(12)));
        try!(self.writer.write_all(&tid));
        try!(self.writer.write_u32::<LittleEndian>(self.index.len() as u32));
        try!(self.writer.flush());
        let mut wpos = self.header_length;
        let mut file = self.file.borrow_mut();
        while wpos < self.length {
            try!(file.seek(io::SeekFrom::Start(wpos)));
            let dlen = try!(file.read_u32::<LittleEndian>());
            try!(file.seek(
                io::SeekFrom::Start(wpos + records::DATA_TID_OFFSET)));
            try!(file.write_all(&tid));
            wpos += records::DATA_HEADER_SIZE + dlen as u64;
        }
        self.tid = tid;
        Ok(())
    }

    pub fn stage(&mut self, tid: Tid, mut out: &mut File, pos: u64)
                 -> io::Result<u64> {
        // Update tids in temp file
        try!(self.save_tid(tid));
        let mut file = self.file.borrow_mut();
        try!(file.seek(io::SeekFrom::Start(0)));

        self.length += 8;
        assert_eq!(try!(io::copy(&mut *file, &mut out)), self.length);

        // Truncate to 0 in hopes of avoiding write to disk
        try!(file.set_len(0));

        self.commit_pos = pos;

        // TODO: free self.file and self.writer
        Ok(self.length)
    }
}

impl<'store, 't> std::fmt::Debug for Transaction<'store> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Transaction()") // TODO: more informative :)
    }
}

type TransactionSerialIteratorItem = io::Result<(Oid, Tid)>;

pub struct TransactionSerialIterator<'t> {
    reader: io::BufReader<File>,
    index: &'t Index,
    length: u64,
    pos: u64,
}

impl<'t> TransactionSerialIterator<'t> {

    fn new(mut file: File,
           index: &'t Index,
           length: u64,
           pos: u64) -> io::Result<TransactionSerialIterator> {
        try!(file.seek(io::SeekFrom::Start(pos)));
        Ok(TransactionSerialIterator {
            reader: io::BufReader::new(file),
            index: index, length: length, pos: pos })
    }

    fn read(&mut self) -> TransactionSerialIteratorItem {
        loop {
            let dlen = try!(self.reader.read_u32::<LittleEndian>());
            let oid = try!(read8(&mut self.reader));
            match self.index.get(&oid) {
                Some(&pos) => {
                    if &pos != &self.pos {
                        // The object was repeated and this isn't the last
                        try!(self.reader.seek(
                            io::SeekFrom::Current(24 + dlen as i64)));
                        self.pos += records::DATA_HEADER_SIZE + dlen as u64;
                        continue
                    }
                },
                None => {
                    return Err(io_error("index fail in transaction"))
                }
            }
            let tid = try!(read8(&mut self.reader));
            try!(self.reader.seek(io::SeekFrom::Current(16 + dlen as i64)));
            self.pos += records::DATA_HEADER_SIZE + dlen as u64;
            return Ok((oid, tid))
        }
    }
}

impl<'t> std::iter::Iterator for TransactionSerialIterator<'t> {

    type Item = TransactionSerialIteratorItem;
        
    fn next(&mut self) -> Option<TransactionSerialIteratorItem> {
        if self.pos < self.length {
            Some(self.read())
        }
        else {
            None
        }
    }
}
