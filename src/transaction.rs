use std;

use errors::*;
use util::*;
use index::Index;
use pool;
use records;

static PADDING16: [u8; 16] = [0u8; 16]; 

pub struct TransactionData<'store> {
    filep: pool::TmpFilePointer<'store>,
    writer: io::BufWriter<File>,
    length: u64,
    header_length: u64,
    needs_to_be_packed: bool,
}

impl<'store> TransactionData<'store> {
    
    pub fn save_tid(&mut self, tid: Tid, count: u32) -> io::Result<()> {
        try!(self.writer.seek(io::SeekFrom::Start(12)));
        try!(self.writer.write_all(&tid));
        try!(self.writer.write_u32::<BigEndian>(count));
        try!(self.writer.flush());
        let mut wpos = self.header_length;
        let mut file = self.filep.borrow_mut();
        while wpos < self.length {
            try!(file.seek(io::SeekFrom::Start(wpos)));
            let dlen = try!(file.read_u32::<BigEndian>());
            try!(file.seek(
                io::SeekFrom::Start(wpos + records::DATA_TID_OFFSET)));
            try!(file.write_all(&tid));
            wpos += records::DATA_HEADER_SIZE + dlen as u64;
        }
        Ok(())
    }

}

pub enum TransactionState<'store> {
    Saving(TransactionData<'store>),
    Transitioning,
    Voting(TransactionData<'store>),
    Voted,
}

pub struct Transaction<'store> {
    pub id: Tid,
    pub state: TransactionState<'store>,
    index: Index,
}

impl<'store, 't> Transaction<'store> {

    pub fn begin(filep: pool::PooledFilePointer<'store, pool::TmpFileFactory>,
                 id: Tid, user: &[u8], desc: &[u8], ext: &[u8])
                 -> io::Result<Transaction<'store>> {
        let mut file = try!(filep.borrow().try_clone());
        try!(file.seek(io::SeekFrom::Start(0)));
        try!(file.set_len(0));
        let mut writer = io::BufWriter::new(file);
        try!(writer.write_all(PADDING_MARKER));
        try!(writer.write_all(&PADDING16)); // tlen, tid
        try!(writer.write_u32::<BigEndian>(0 as u32)); // count
        try!(writer.write_u16::<BigEndian>(user.len() as u16));
        try!(writer.write_u16::<BigEndian>(desc.len() as u16));
        try!(writer.write_u32::<BigEndian>(ext.len() as u32));
        if user.len() > 0 { try!(writer.write_all(user)) }
        if desc.len() > 0 { try!(writer.write_all(desc)) }
        if  ext.len() > 0 { try!(writer.write_all(ext)) }
        let length = 4u64 + records::TRANSACTION_HEADER_LENGTH +
            user.len() as u64 + desc.len() as u64 + ext.len() as u64;
        Ok(Transaction {
            id: id, index: Index::new(),
            state: TransactionState::Saving(TransactionData {
                filep: filep, writer: writer,
                length: length, header_length: length,
                needs_to_be_packed: false,
            }),
        })
    }

    pub fn save(&mut self, oid: Oid, serial: Tid, data: &[u8])
                -> io::Result<()> {
        // Save data in the first phase of 2-phase commit.
        if let TransactionState::Saving(ref mut  tdata) = self.state {
            try!(tdata.writer.write_u32::<BigEndian>(data.len() as u32));
            try!(tdata.writer.write_all(&oid));
            // read tid now, committed later:
            try!(tdata.writer.write_all(&serial));
            try!(write_u64(&mut tdata.writer, 0)); // previous
            try!(write_u64(&mut tdata.writer, tdata.length)); // offset
            if data.len() > 0 { try!(tdata.writer.write_all(data)) }
            if self.index.insert(oid, tdata.length).is_some() {
                // There was an earlier save for this oid.  We'll want to
                // pack the data before committing.
                tdata.needs_to_be_packed = true;
            };
            tdata.length += records::DATA_HEADER_SIZE + data.len() as u64;
            Ok(())
        }
        else { Err(io_error("Invalid trans state")) }
    }

    pub fn lock_data(&self) -> Result<(Tid, Vec<Oid>)> {
        if let TransactionState::Saving(_) = self.state {
            let mut oids =
                self.index.keys().map(| r | r.clone()).collect::<Vec<Oid>>();
            oids.reverse();
            Ok((self.id, oids))
        }          
        else { Err("Invalid trans state".into()) }
    }

    pub fn locked(&mut self) -> Result<()>
    {
        let mut state = TransactionState::Transitioning;
        std::mem::swap(&mut state, &mut self.state);

        if let TransactionState::Saving(mut data) = state {
            match data.writer.flush().chain_err(|| "trans writer flush") {
                Ok(_) => {
                    self.state = TransactionState::Voting(data);
                    Ok(())
                }
                err => {
                    self.state = TransactionState::Saving(data);
                    err
                },
            }
        }          
        else {
            std::mem::swap(&mut state, &mut self.state); // restore
            Err("Invalid trans state".into())
        }
    }

    pub fn unlocked(&mut self) -> Result<()> {
        let mut state = TransactionState::Transitioning;
        std::mem::swap(&mut state, &mut self.state);
        if let TransactionState::Voting(mut data) = state {
            match seek(&mut data.writer, data.length) {
                Ok(_) => {
                    self.state = TransactionState::Saving(data);
                    Ok(())
                }
                err => {
                    self.state = TransactionState::Voting(data);
                    Err("seek failed".into())
                },
            }
        }          
        else {
            std::mem::swap(&mut state, &mut self.state); // restore
            Err("Invalid trans state".into())
        }

    }

    pub fn serials(&'t mut self) -> io::Result<TransactionSerialIterator<'t>> {
        if let TransactionState::Voting(ref mut data) = self.state {
            TransactionSerialIterator::new(
                try!(data.filep.borrow().try_clone()),
                &self.index, data.length, data.header_length)
        }
        else { Err(io_error("Invalid trans state")) }
    }

    pub fn get_data(&mut self, oid: &Oid) -> Result<Bytes> {
        if let TransactionState::Voting(ref mut data) = self.state {
            let pos = try!(
                self.index.get(oid).ok_or(Error::from("trans index error")));
            let mut file = data.filep.borrow_mut();
            try!(file.seek(io::SeekFrom::Start(*pos))
                 .chain_err(|| "trans seek"));
            let dlen = try!(file.read_u32::<BigEndian>()
                            .chain_err(|| "trans read dlen"));
            let data = if dlen > 0 {
                try!(file.seek(
                    io::SeekFrom::Start(pos + records::DATA_HEADER_SIZE))
                     .chain_err(|| "trans seek data"));
                try!(read_sized(&mut *file, dlen as usize)
                     .chain_err(|| "trans read data"))
            }
            else {
                vec![0u8; 0]
            };
            Ok(data)
        }          
        else { Err("Invalid trans state".into()) }
    }

    pub fn set_previous(&mut self, oid: &Oid, previous: u64) -> Result<()> {
        if let TransactionState::Voting(ref mut data) = self.state {
            let pos = try!(
                self.index.get(oid)
                    .ok_or(Error::from("trans index error")));
            let mut file = data.filep.borrow_mut();
            try!(file.seek(
                io::SeekFrom::Start(pos + records::DATA_PREVIOUS_OFFSET))
                 .chain_err(|| "trans seek prev"));
            try!(file.write_u64::<BigEndian>(previous)
                 .chain_err(|| "trans write previous"));
            Ok(())
        }          
        else { Err("Invalid trans state".into()) }
    }
    
    pub fn pack(&mut self) -> io::Result<()> {
        // If necessary, pack out records that were overwritten.
        // Also write length into header.
        if let TransactionState::Voting(ref mut data) = self.state {
            let mut file = data.filep.borrow_mut();

            if data.needs_to_be_packed {
                let mut rpos = data.header_length;
                let mut wpos = data.header_length;

                let mut buf = [0u8; 12];
                while rpos < data.length {
                    try!(file.seek(io::SeekFrom::Start(rpos)));
                    try!(file.read_exact(&mut buf));
                    let dlen = BigEndian::read_u32(&buf) as u64;
                    let oid = try!(read8(&mut &buf[4..]));
                    let oid_pos =
                        try!(self.index.get(&oid)
                             .ok_or(io_error("trans index get"))).clone();
                    if oid_pos == rpos {
                        // We want this one
                        if rpos != wpos {
                            // We need to move it.
                            let mut rest = // tid, previous, offset, data
                                try!(read_sized(
                                    &mut *file,
                                    dlen as usize +
                                        records::DATA_HEADER_SIZE as usize
                                        - 12));
                            // update offset:
                            write_u64(&mut &mut rest[16..24], wpos);
                            try!(file.seek(io::SeekFrom::Start(wpos)));
                            try!(file.write_all(&buf));
                            try!(file.write_all(&rest));
                            self.index.insert(oid, wpos);
                            wpos += dlen + records::DATA_HEADER_SIZE;
                        }
                    }
                    rpos += dlen + records::DATA_HEADER_SIZE;
                }
                try!(file.set_len(wpos));
                data.length = wpos;
            }

            // Update header w length
            let full_length = data.length + 8;
            try!(file.seek(io::SeekFrom::Start(data.length)));
            try!(file.write_u64::<BigEndian>(full_length));
            try!(file.seek(io::SeekFrom::Start(4)));
            try!(file.write_u64::<BigEndian>(full_length));

            Ok(())
        }          
        else { Err(io_error("Invalid trans state")) }
    }

    pub fn stage(&mut self, tid: Tid, mut out: &mut File)
                 -> io::Result<(Index, u64)> {
        let length =
            if let TransactionState::Voting(ref mut data) = self.state {
                // Update tids in temp file
                try!(data.save_tid(tid, self.index.len() as u32));
                let mut file = data.filep.borrow_mut();
                try!(file.seek(io::SeekFrom::Start(0)));
                
                data.length += 8;
                assert_eq!(try!(io::copy(&mut *file, &mut out)), data.length);
                
                // Truncate to 0 in hopes of avoiding write to disk
                try!(file.set_len(0));
                data.length
            }
        else {
            return Err(io_error("Invalid trans state"))
        };
        self.state = TransactionState::Voted;

        let mut index = Index::new();
        std::mem::swap(&mut index, &mut self.index);

        Ok((index, length))
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
            let dlen = try!(self.reader.read_u32::<BigEndian>());
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


// ======================================================================

#[cfg(test)]
pub mod tests {

    use super::*;
    use index;
    use pool;
    use records;
    use util;
    use util::*;
    
    #[test]
    fn works_w_dup() {
        let tmpdir = util::test::dir();

        let pool = pool::FilePool::new(
            pool::TmpFileFactory::base(
                String::from(
                    tmpdir.path().join("tmp").to_str().unwrap())).unwrap(),
            22);

        let tempfilep = pool.get().unwrap();
        let tempfile = tempfilep.borrow().try_clone().unwrap();

        let mut trans = Transaction::begin(
            tempfilep, p64(1234567890), b"user", b"desc", b"{}").unwrap();

        trans.save(p64(0), p64(123456789), &[1; 11]).unwrap();
        trans.save(p64(1), p64(12345678),  &[2; 22]).unwrap();
        trans.save(p64(0), p64(123456788), &[3; 33]).unwrap();
        assert_eq!(trans.lock_data().unwrap(),
                   (p64(1234567890), vec![p64(1), p64(0)]));
        trans.locked().unwrap();
        let mut serials = trans.serials().unwrap()
            .map(| r | r.unwrap())
            .collect::<Vec<(Oid, Tid)>>();
        serials.sort();
        assert_eq!(serials,
                   vec![(p64(0), p64(123456788)), (p64(1), p64(12345678))]);
        assert_eq!(trans.get_data(&p64(0)).unwrap(), vec![3; 33]);
        assert_eq!(trans.get_data(&p64(1)).unwrap(), vec![2; 22]);
        trans.set_previous(&p64(0), 7777);
        
        trans.pack().unwrap();

        let t2 = pool.get().unwrap();
        let mut file = t2.borrow_mut();
        let (index, tsize) = trans.stage(p64(1234567891), &mut file).unwrap();

        // Now, we'll verify the saved data.
        let l = file.seek(io::SeekFrom::End(0)).unwrap();
        assert_eq!(tsize, l);
        seek(&mut *file, 0).unwrap();
        assert_eq!(&read4(&mut *file).unwrap(), b"PPPP");
        let th = records::TransactionHeader::read(&mut *file).unwrap();
        assert_eq!(
            th,
            records::TransactionHeader {
                length: l, id: p64(1234567891), ndata: 2,
                luser: 4, ldesc: 4, lext: 2 });
        assert_eq!(&read4(&mut *file).unwrap(), b"user");
        assert_eq!(&read4(&mut *file).unwrap(), b"desc");
        assert_eq!(&read_sized(&mut *file, 2).unwrap(), b"{}");

        let dh1 = records::DataHeader::read(&mut *file).unwrap();
        assert_eq!(
            dh1,
            records::DataHeader {
                length: 22, id: p64(1), tid: p64(1234567891),
                previous: 0,
                offset: records::TRANSACTION_HEADER_LENGTH + 14,
            });
        assert_eq!(read_sized(&mut *file, dh1.length as usize).unwrap(),
                   vec![2; 22]);

        let dh0 = records::DataHeader::read(&mut *file).unwrap();
        assert_eq!(
            dh0,
            records::DataHeader {
                length: 33, id: p64(0), tid: p64(1234567891),
                previous: 7777,
                offset:
                dh1.offset + records::DATA_HEADER_SIZE + dh1.length as u64,
            });
        assert_eq!(read_sized(&mut *file, dh0.length as usize).unwrap(),
                   vec![3; 33]);

        assert_eq!(read_u64(&mut *file).unwrap(), l); // Check redundant length

        assert_eq!(
            index, {
                let mut other = index::Index::new();
                other.insert(p64(0), dh0.offset);
                other.insert(p64(1), dh1.offset);
                other
            });
    }
    
    #[test]
    fn works_wo_dup() {
        let tmpdir = util::test::dir();

        let pool = pool::FilePool::new(
            pool::TmpFileFactory::base(
                String::from(
                    tmpdir.path().join("tmp").to_str().unwrap())).unwrap(),
            22);

        let tempfilep = pool.get().unwrap();
        let tempfile = tempfilep.borrow().try_clone().unwrap();

        let mut trans = Transaction::begin(
            tempfilep, p64(1234567890), b"user", b"desc", b"{}").unwrap();

        trans.save(p64(0), p64(123456789), &[1; 11]).unwrap();
        trans.save(p64(1), p64(12345678),  &[2; 22]).unwrap();
        assert_eq!(trans.lock_data().unwrap(),
                   (p64(1234567890), vec![p64(1), p64(0)]));
        trans.locked().unwrap();
        let mut serials = trans.serials().unwrap()
            .map(| r | r.unwrap())
            .collect::<Vec<(Oid, Tid)>>();
        serials.sort();
        assert_eq!(serials,
                   vec![(p64(0), p64(123456789)), (p64(1), p64(12345678))]);
        assert_eq!(trans.get_data(&p64(0)).unwrap(), vec![1; 11]);
        assert_eq!(trans.get_data(&p64(1)).unwrap(), vec![2; 22]);
        trans.set_previous(&p64(0), 7777);
        
        trans.pack().unwrap();

        let t2 = pool.get().unwrap();

        assert_eq!(pool.len(), 0);
        
        let mut file = t2.borrow_mut();
        let (index, tsize) = trans.stage(p64(1234567891), &mut file).unwrap();

        assert_eq!(pool.len(), 1); // The transaction's tmp file ws returned.

        // Now, we'll verify the saved data.
        let l = file.seek(io::SeekFrom::End(0)).unwrap();
        assert_eq!(tsize, l);
        seek(&mut *file, 0).unwrap();
        assert_eq!(&read4(&mut *file).unwrap(), b"PPPP");
        let th = records::TransactionHeader::read(&mut *file).unwrap();
        assert_eq!(
            th,
            records::TransactionHeader {
                length: l, id: p64(1234567891), ndata: 2,
                luser: 4, ldesc: 4, lext: 2 });
        assert_eq!(&read4(&mut *file).unwrap(), b"user");
        assert_eq!(&read4(&mut *file).unwrap(), b"desc");
        assert_eq!(&read_sized(&mut *file, 2).unwrap(), b"{}");

        let dh0 = records::DataHeader::read(&mut *file).unwrap();
        assert_eq!(
            dh0,
            records::DataHeader {
                length: 11, id: p64(0), tid: p64(1234567891),
                previous: 7777,
                offset: records::TRANSACTION_HEADER_LENGTH + 14,
            });
        assert_eq!(read_sized(&mut *file, dh0.length as usize).unwrap(),
                   vec![1; 11]);

        let dh1 = records::DataHeader::read(&mut *file).unwrap();
        assert_eq!(
            dh1,
            records::DataHeader {
                length: 22, id: p64(1), tid: p64(1234567891),
                previous: 0,
                offset:
                dh0.offset + records::DATA_HEADER_SIZE + dh0.length as u64,
            });
        assert_eq!(read_sized(&mut *file, dh1.length as usize).unwrap(),
                   vec![2; 22]);

        assert_eq!(read_u64(&mut *file).unwrap(), l); // Check redundant length

        assert_eq!(
            index, {
                let mut other = index::Index::new();
                other.insert(p64(0), dh0.offset);
                other.insert(p64(1), dh1.offset);
                other
            });
    }
}
