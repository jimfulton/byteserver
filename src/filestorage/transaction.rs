use std;

use super::errors::*;
use super::util::*;
use super::index::Index;
use super::pool;
use super::records;

static PADDING24: [u8; 24] = [0u8; 24]; 

#[derive(Debug)]
pub struct Transaction<'store> {
    pub id: Tid, // internal temporary tid
    file: pool::PooledFilePointer<'store, pool::TmpFileFactory>,
    writer: io::BufWriter<File>,
    save_length: u64,
    length: u64,
    index: Index,
}

impl<'store, 't> Transaction<'store> {

    pub fn begin(file: pool::PooledFilePointer<'store, pool::TmpFileFactory>,
                 id: Tid, user: &[u8], desc: &[u8], ext: &[u8])
                 -> io::Result<Transaction<'store>> {
        let mut writer = io::BufWriter::new(try!(file.borrow().try_clone()));
        try!(writer.write_u16::<LittleEndian>(user.len() as u16));
        try!(writer.write_u16::<LittleEndian>(desc.len() as u16));
        try!(writer.write_u32::<LittleEndian>(ext.len() as u32));
        if user.len() > 0 { try!(writer.write_all(user)) }
        if desc.len() > 0 { try!(writer.write_all(desc)) }
        if  ext.len() > 0 { try!(writer.write_all(ext)) }
        let length = records::TRANSACTION_HEADER_LENGTH +
            user.len() as u64 + desc.len() as u64 + ext.len() as u64;
        Ok(Transaction {
            file: file, writer: writer, index: Index::new(),
            id: id, length: length, save_length: length}
        )
    }

    pub fn save(&mut self, oid: Oid, data: &[u8])
                -> io::Result<()> {
        // Save data in the first phase of 2-phase commit.
        try!(self.writer.write_u32::<LittleEndian>(data.len() as u32));
        try!(self.writer.write_all(&oid));
        try!(self.writer.write_all(&PADDING24)); // tid, previous, and offset
        if data.len() > 0 { try!(self.writer.write_all(data)) }
        self.index.insert(oid, self.save_length);
        self.save_length += records::DATA_HEADER_SIZE + data.len() as u64;
        Ok(())
    }

    pub fn write(&mut self, oid: &Oid, tid: &Tid, previous: u64, data: &[u8])
             -> io::Result<()> {
        // (over)-write data in the second phase of two-phase commit
        try!(self.writer.write_u32::<LittleEndian>(data.len() as u32));
        try!(self.writer.write_all(oid));
        try!(self.writer.write_all(tid));
        try!(self.writer.write_u64::<LittleEndian>(previous));
        try!(self.writer.write_u64::<LittleEndian>(self.length));
        if data.len() > 0 { try!(self.writer.write_all(data)) }
        self.length += records::DATA_HEADER_SIZE + data.len() as u64;
        Ok(())
    }

    pub fn vote(&mut self, tid: &Tid, mut writer: &mut io::Write )
                -> io::Result<()> {
        let mut source = self.file.borrow_mut();
        try!(source.seek(io::SeekFrom::Start(0)));
        self.length += 8;
        try!(writer.write_all(PADDING_MARKER));
        try!(writer.write_u64::<LittleEndian>(self.length));
        try!(writer.write_all(tid));
        try!(writer.write_u32::<LittleEndian>(self.index.len() as u32));
        try!(io::copy(&mut *source, &mut writer));
        Ok(())
    }

    pub fn vote_locked(&mut self) {}

    pub fn saved(&'t self) -> Result<TransactionIterator<'t>> {
        Ok(TransactionIterator {
            reader: io::BufReader::new(
                try!(self.file.borrow().try_clone()
                     .chain_err(|| "Failed to clone trans it file"))),
            index: &self.index, length: self.save_length, pos: self.length })
    }

}

impl<'store> std::cmp::PartialEq for Transaction<'store> {
    fn eq(&self, other: &Transaction) -> bool { self.id == other.id }
} 

impl<'store> std::hash::Hash for Transaction<'store> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

type TransactionIteratorItem = io::Result<(Oid, Bytes)>;

pub struct TransactionIterator<'t> {
    reader: io::BufReader<File>,
    index: &'t Index,
    length: u64,
    pos: u64,
}

impl<'t> TransactionIterator<'t> {

    fn read(&mut self) -> TransactionIteratorItem {
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
                    else {
                    }
                },
                None => {
                    return io_error("index fail in transaction")
                }
            }
            try!(self.reader.seek(io::SeekFrom::Current(24)));
            let data = if dlen > 0 {
                try!(read_sized(&mut self.reader, dlen as usize))
            }
            else {
                vec![0u8; 0]
            };
            self.pos += records::DATA_HEADER_SIZE + dlen as u64;
            return Ok((oid, data))
        }
    }
}

impl<'t> std::iter::Iterator for TransactionIterator<'t> {

    type Item = TransactionIteratorItem;
        
    fn next(&mut self) -> Option<TransactionIteratorItem> {
        if self.pos < self.length {
            Some(self.read())
        }
        else {
            None
        }
    }
}


// ======================================================================

#[cfg(test)]
mod tests {

    use super::*;
    use super::super::tid;
    use super::super::pool;
    use std::hash::{Hash, SipHasher, Hasher};

    #[test]
    fn test_hash_eq() {
        let tid1 = tid::now_tid();
        let tid2 = tid::later_than(tid1, tid1);
        let f = pool::TmpFileFactory::base(String::from(".")).unwrap();
        let pool = pool::FilePool::new(f, 3);
        let t11 = Transaction::begin(pool.get().unwrap(),
                                     tid1, b"q", b"w", b"e").unwrap();
        let t21 = Transaction::begin(pool.get().unwrap(),
                                     tid2, b"r", b"t", b"y").unwrap();
        let t12 = Transaction::begin(pool.get().unwrap(),
                                     tid1, b"u", b"i", b"o").unwrap();

        assert_eq!(t11, t12); assert!(t11 != t21);

        fn hash(v: &Transaction) -> u64 {
            let mut hasher = SipHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }
        assert_eq!(hash(&t11), hash(&t12));
        assert!(hash(&t11) != hash(&t21));
    }
}
