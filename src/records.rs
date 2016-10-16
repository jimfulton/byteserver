use index;
use util::*;

pub static HEADER_MARKER: &'static [u8] = b"fs2 ";

pub struct FileHeader {
    alignment: u64,
    previous: String,
}
pub const HEADER_SIZE: u64 = 4096;

impl FileHeader {

    pub fn new() -> FileHeader {
        FileHeader { alignment: 1 << 32, previous: String::new() }
    }

    pub fn read<T>(mut reader: &mut T) -> io::Result<FileHeader>
        where T: io::Read + io::Seek
    {
        check_magic(&mut reader, HEADER_MARKER);
        io_assert!(try!(reader.read_u64::<LittleEndian>()) == 4096,
                   "Bad header length");
        let alignment = try!(reader.read_u64::<LittleEndian>());
        let h = match String::from_utf8(try!(read_sized16(&mut reader))) {
            Ok(previous) =>
                FileHeader { alignment: alignment, previous: previous },
            _ => return Err(io_error("Bad previous utf8")),
        };
        io_assert!(try!(reader.seek(io::SeekFrom::Start(4088))) == 4088,
                   "Seek failed");
        io_assert!(try!(reader.read_u64::<LittleEndian>()) == 4096,
                   "Bad header extra length");
        Ok(h)
    }

    pub fn write<T>(&self, writer: &mut T) -> io::Result<()>
        where T: io::Write + io::Seek
    {
        try!(writer.write_all(&HEADER_MARKER));
        try!(writer.write_u64::<LittleEndian>(4096));
        try!(writer.write_u64::<LittleEndian>(self.alignment));
        try!(writer.write_u16::<LittleEndian>(self.previous.len() as u16));
        if self.previous.len() > 0 {
            try!(writer.write_all(&self.previous.clone().into_bytes()));
        }
        io_assert!(
            try!(writer.seek(io::SeekFrom::Start(4088))) == 4088,
            "seek failed"
        );
        try!(writer.write_u64::<LittleEndian>(4096));
        Ok(())
    }
}

#[derive(PartialEq, Debug)]
pub struct TransactionHeader {
    pub length: u64,
    pub id: Tid,
    pub ndata: u32,
    pub luser: u16,
    pub ldesc: u16,
    pub lext: u32,
}
pub const TRANSACTION_HEADER_LENGTH: u64 = 28;

impl TransactionHeader {

    fn new(tid: Tid) -> TransactionHeader {
        TransactionHeader {
            length: 0, id: tid, luser: 0, ldesc: 0, lext: 0, ndata: 0 }
    }

    pub fn read(mut reader: &mut io::Read) -> io::Result<TransactionHeader> {
        let length = try!(reader.read_u64::<LittleEndian>());
        let mut h = TransactionHeader::new(try!(read8(&mut reader)));
        h.length = length;
        h.ndata = try!(reader.read_u32::<LittleEndian>());
        h.luser = try!(reader.read_u16::<LittleEndian>());
        h.ldesc = try!(reader.read_u16::<LittleEndian>());
        h.lext = try!(reader.read_u32::<LittleEndian>());
        Ok(h)
    }

    pub fn update_index<T>(&self, mut reader: &mut T, index: &mut index::Index)
                           -> io::Result<()>
        where T: io::Read + io::Seek {
        let mut pos =
            try!(reader.seek(io::SeekFrom::Current(
                self.luser as i64 + self.ldesc as i64 + self.lext as i64)));

        for i in 0 .. self.ndata {
            let ldata = try!(reader.read_u32::<LittleEndian>());
            index.insert(try!(read8(&mut reader)), pos);
            pos += 24 + ldata as u64;
            if i + 1 < self.ndata {
                try!(seek(&mut reader, pos));
            }
        }

        Ok(())
    }
    
}

#[derive(PartialEq, Debug)]
pub struct DataHeader {
    pub length: u32,
    pub id: Oid,
    pub tid: Tid,
    pub previous: u64,
    pub offset: u64,
}
pub const DATA_HEADER_SIZE: u64 = 36;
pub const DATA_TID_OFFSET: u64 = 12;
pub const DATA_PREVIOUS_OFFSET: u64 = 20;

impl DataHeader {

    fn new(tid: Tid) -> TransactionHeader {
        TransactionHeader {
            length: 0, id: tid, luser: 0, ldesc: 0, lext: 0, ndata: 0 }
    }

    pub fn read(reader: &mut io::Read) -> io::Result<DataHeader> {
        // assume reader is unbuffered
        let mut buf = [0u8; DATA_HEADER_SIZE as usize];
        try!(reader.read_exact(&mut buf));
        Ok(DataHeader {
            length: LittleEndian::read_u32(&buf[0..4]),
            id: try!(read8(&mut &buf[4..])),
            tid: try!(read8(&mut &buf[12..])),
            previous: LittleEndian::read_u64(&buf[20..]),
            offset: LittleEndian::read_u64(&buf[28..]),
        })
    }
}


    

// ======================================================================

#[cfg(test)]
mod tests {

    pub use super::*;
    use util::*;

    fn file_header_sample(previous: &[u8]) -> Vec<u8> {
        let mut sample = vec![0u8; 0];
        sample.extend_from_slice(&HEADER_MARKER);
        sample.extend_from_slice(&[0, 16, 0, 0, 0, 0, 0, 0]); // 4096
        sample.extend_from_slice(&[0, 0, 0, 64, 0, 0, 0, 0]); // 1<<30
        sample.extend_from_slice(&vec![previous.len() as u8, 0u8][..]);
        sample.extend_from_slice(&previous);
        sample.extend_from_slice(&vec![0; 4066 - previous.len()]);
        sample.extend_from_slice(&[0, 16, 0, 0, 0, 0, 0, 0]); // 4096
        sample
    } 

    #[test]
    fn read_file_header() {
        let mut reader = io::Cursor::new(file_header_sample(b""));
        let h = FileHeader::read(&mut reader).unwrap();
        assert_eq!(&h.previous, "");
        assert_eq!(h.alignment, 1<<30);

        let mut reader = io::Cursor::new(file_header_sample(b"previous"));
        let h = FileHeader::read(&mut reader).unwrap();
        assert_eq!(h.previous, "previous");
        assert_eq!(h.alignment, 1<<30);
    }

    #[test]
    fn write_file_header() {
        
        let mut writer = io::Cursor::new(vec![0u8; 0]);
        let h = FileHeader {
            previous: String::new(),
            alignment: 1<<30,
        };
        h.write(&mut writer).unwrap();
        assert_eq!(writer.into_inner(), file_header_sample(b""));
        
        let mut writer = io::Cursor::new(vec![0u8; 0]);
        let h = FileHeader {
            previous: String::from("previous"),
            alignment: 1<<30,
        };
        h.write(&mut writer).unwrap();
        assert_eq!(writer.into_inner(), file_header_sample(b"previous"));
    }

    #[test]
    fn read_transaction_header() {
        // Note that the transaction-header read method is called
        // after reading the record marker.

        let mut cursor = io::Cursor::new(Vec::new());

        // Write out some sample data:
        write_u64(&mut cursor, 9999).unwrap();
        cursor.write_all(&p64(1234567890)).unwrap();
        write_u32(&mut cursor, 2).unwrap();
        write_u16(&mut cursor, 11).unwrap();
        write_u16(&mut cursor, 22).unwrap();
        write_u32(&mut cursor, 33).unwrap();
        seek(&mut cursor, 0).unwrap();

        let h = TransactionHeader::read(&mut cursor).unwrap();
        assert_eq!(
            h,
            TransactionHeader {
                length: 9999, id: p64(1234567890), ndata: 2,
                luser: 11, ldesc: 22, lext: 33,
            });
    }
    
}
