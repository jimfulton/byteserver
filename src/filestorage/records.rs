use super::index;
use super::util::*;

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
            _ => return io_error("Bad previous utf8"),
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

    pub fn update_index<T>(
        &self, mut reader: &mut T, index: &mut index::Index)
        -> io::Result<()>
        where T: io::Read + io::Seek {
        let mut pos = try!(reader.seek(
            io::SeekFrom::Current(
                self.luser as i64 + self.ldesc as i64 + self.lext as i64)));

        for i in 0 .. self.ndata {
            let ldata = try!(reader.read_u32::<LittleEndian>());
            index.insert(try!(read8(&mut reader)), pos);
            pos += 24 + ldata as u64;
        }

        Ok(())
    }
    
}

pub struct DataHeader {
    pub length: u32,
    pub id: Oid,
    pub tid: Tid,
    pub previous: u64,
    pub offset: u64,
}
pub const DATA_HEADER_SIZE: u64 = 36;

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
    pub use std::fs::File;
    pub use tempdir::TempDir;
    pub use std::io;

    describe! fileheader {
        before_each {
            let mut sample = vec![0u8; 0];
            sample.extend_from_slice(&HEADER_MARKER);
            sample.extend_from_slice(&[0, 16, 0, 0, 0, 0, 0, 0]); // 4096
            sample.extend_from_slice(&[0, 0, 0, 64, 0, 0, 0, 0]); // 1<<30
            sample.extend_from_slice(&[0, 0]);                    // len(b'')
            sample.extend_from_slice(&[0; 4066]);
            sample.extend_from_slice(&[0, 16, 0, 0, 0, 0, 0, 0]); // 4096 again

            let previous: &[u8] = b"previous";
            let mut sample_prev = vec![0u8; 0];
            sample_prev.extend_from_slice(&HEADER_MARKER);
            sample_prev.extend_from_slice(&[0, 16, 0, 0, 0, 0, 0, 0]); // 4096
            sample_prev.extend_from_slice(&[0, 0, 0, 64, 0, 0, 0, 0]); // 1<<30
            sample_prev.extend_from_slice(&[8, 0]);
            sample_prev.extend_from_slice(&previous);
            sample_prev.extend_from_slice(&[0; 4058]);
            sample_prev.extend_from_slice(&[0, 16, 0, 0, 0, 0, 0, 0]); // 4096
        }

        it "can be read" {
            let mut reader = io::Cursor::new(sample);
            let h = FileHeader::read(&mut reader).unwrap();
            assert_eq!(&h.previous, "");
            assert_eq!(h.alignment, 1<<30);

            println!("{:?}", previous);
            
            let mut reader = io::Cursor::new(sample_prev);
            let h = FileHeader::read(&mut reader).unwrap();
            assert_eq!(h.previous,
                       String::from_utf8(previous.to_vec()).unwrap());
            assert_eq!(h.alignment, 1<<30);
        }
        
        it "can be written" {
            
            let mut writer = io::Cursor::new(vec![0u8; 0]);
            let h = FileHeader {
                previous: String::new(),
                alignment: 1<<30,
            };
            h.write(&mut writer).unwrap();
            assert_eq!(writer.into_inner(), sample.to_vec());
            
            let mut writer = io::Cursor::new(vec![0u8; 0]);
            let h = FileHeader {
                previous: String::from("previous"),
                alignment: 1<<30,
            };
            h.write(&mut writer).unwrap();
            assert_eq!(writer.into_inner(), sample_prev.to_vec());
        }
    }
}
