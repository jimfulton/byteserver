#[allow(unused_imports)]
use std::io::prelude::*;

use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};

use crate::index;
use crate::util;

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

    pub fn read<T>(mut reader: &mut T) -> std::io::Result<FileHeader>
        where T: std::io::Read + std::io::Seek
    {
        util::check_magic(&mut reader, HEADER_MARKER);
        util::io_assert(reader.read_u64::<BigEndian>()? == 4096,
                  "Bad header length")?;
        let alignment = reader.read_u64::<BigEndian>()?;
        let h = match String::from_utf8(util::read_sized16(&mut reader)?) {
            Ok(previous) =>
                FileHeader { alignment: alignment, previous: previous },
            _ => return Err(util::io_error("Bad previous utf8")),
        };
        util::io_assert(reader.seek(std::io::SeekFrom::Start(4088))? == 4088,
                  "Seek failed")?;
        util::io_assert(reader.read_u64::<BigEndian>()? == 4096,
                  "Bad header extra length")?;
        Ok(h)
    }

    pub fn write<T>(&self, writer: &mut T) -> std::io::Result<()>
        where T: std::io::Write + std::io::Seek
    {
        writer.write_all(&HEADER_MARKER)?;
        writer.write_u64::<BigEndian>(4096)?;
        writer.write_u64::<BigEndian>(self.alignment)?;
        writer.write_u16::<BigEndian>(self.previous.len() as u16)?;
        if self.previous.len() > 0 {
            writer.write_all(&self.previous.clone().into_bytes())?;
        }
        util::io_assert(
            writer.seek(std::io::SeekFrom::Start(4088))? == 4088,
            "seek failed"
        )?;
        writer.write_u64::<BigEndian>(4096)?;
        Ok(())
    }
}

#[derive(PartialEq, Debug)]
pub struct TransactionHeader {
    pub length: u64,
    pub id: util::Tid,
    pub ndata: u32,
    pub luser: u16,
    pub ldesc: u16,
    pub lext: u32,
}
pub const TRANSACTION_HEADER_LENGTH: u64 = 28;

impl TransactionHeader {

    fn new(tid: util::Tid) -> TransactionHeader {
        TransactionHeader {
            length: 0, id: tid, luser: 0, ldesc: 0, lext: 0, ndata: 0 }
    }

    pub fn read(mut reader: &mut dyn std::io::Read)
                -> std::io::Result<TransactionHeader> {
        let length = reader.read_u64::<BigEndian>()?;
        let mut h = TransactionHeader::new(util::read8(&mut reader)?);
        h.length = length;
        h.ndata = reader.read_u32::<BigEndian>()?;
        h.luser = reader.read_u16::<BigEndian>()?;
        h.ldesc = reader.read_u16::<BigEndian>()?;
        h.lext = reader.read_u32::<BigEndian>()?;
        Ok(h)
    }

    pub fn update_index<T>(&self, mut reader: &mut T, index: &mut index::Index,
                           mut last_oid: util::Oid)
                           -> std::io::Result<util::Oid>
        where T: std::io::Read + std::io::Seek {
        let mut pos =
            reader.seek(
                std::io::SeekFrom::Current(
                    self.luser as i64 + self.ldesc as i64 + self.lext as i64))?;

        for i in 0 .. self.ndata {
            let ldata = reader.read_u32::<BigEndian>()?;
            let oid = util::read8(&mut reader)?;
            index.insert(oid, pos);
            if oid > last_oid {
                last_oid = oid;
            }
            pos += DATA_HEADER_SIZE + ldata as u64;
            if i + 1 < self.ndata {
                util::seek(&mut reader, pos)?;
            }
        }

        Ok(last_oid)
    }
    
}

#[derive(PartialEq, Debug)]
pub struct DataHeader {
    pub length: u32,
    pub id: util::Oid,
    pub tid: util::Tid,
    pub previous: u64,
    pub offset: u64,
}
pub const DATA_HEADER_SIZE: u64 = 36;
pub const DATA_TID_OFFSET: u64 = 12;
pub const DATA_PREVIOUS_OFFSET: u64 = 20;

impl DataHeader {

    fn new(tid: util::Tid) -> TransactionHeader {
        TransactionHeader {
            length: 0, id: tid, luser: 0, ldesc: 0, lext: 0, ndata: 0 }
    }

    pub fn read(reader: &mut dyn std::io::Read) -> std::io::Result<DataHeader> {
        // assume reader is unbuffered
        let mut buf = [0u8; DATA_HEADER_SIZE as usize];
        reader.read_exact(&mut buf)?;
        Ok(DataHeader {
            length: BigEndian::read_u32(&buf[0..4]),
            id: util::read8(&mut &buf[4..])?,
            tid: util::read8(&mut &buf[12..])?,
            previous: BigEndian::read_u64(&buf[20..]),
            offset: BigEndian::read_u64(&buf[28..]),
        })
    }
}


    

// ======================================================================

#[cfg(test)]
mod tests {

    pub use super::*;

    fn file_header_sample(previous: &[u8]) -> Vec<u8> {
        let mut sample = vec![0u8; 0];
        sample.extend_from_slice(&HEADER_MARKER);
        sample.extend_from_slice(&[0, 0, 0, 0, 0, 0, 16, 0]); // 4096
        sample.extend_from_slice(&[0, 0, 0, 0, 64, 0, 0, 0]); // 1<<30
        sample.extend_from_slice(&vec![0u8, previous.len() as u8][..]);
        sample.extend_from_slice(&previous);
        sample.extend_from_slice(&vec![0; 4066 - previous.len()]);
        sample.extend_from_slice(&[0, 0, 0, 0, 0, 0, 16, 0]); // 4096
        sample
    } 

    #[test]
    fn read_file_header() {
        let mut reader = std::io::Cursor::new(file_header_sample(b""));
        let h = FileHeader::read(&mut reader).unwrap();
        assert_eq!(&h.previous, "");
        assert_eq!(h.alignment, 1<<30);

        let mut reader = std::io::Cursor::new(file_header_sample(b"previous"));
        let h = FileHeader::read(&mut reader).unwrap();
        assert_eq!(h.previous, "previous");
        assert_eq!(h.alignment, 1<<30);
    }

    #[test]
    fn write_file_header() {
        
        let mut writer = std::io::Cursor::new(vec![0u8; 0]);
        let h = FileHeader {
            previous: String::new(),
            alignment: 1<<30,
        };
        h.write(&mut writer).unwrap();
        assert_eq!(writer.into_inner(), file_header_sample(b""));
        
        let mut writer = std::io::Cursor::new(vec![0u8; 0]);
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

        let mut cursor = std::io::Cursor::new(Vec::new());

        // Write out some sample data:
        util::write_u64(&mut cursor, 9999).unwrap();
        cursor.write_all(&util::p64(1234567890)).unwrap();
        util::write_u32(&mut cursor, 2).unwrap();
        util::write_u16(&mut cursor, 11).unwrap();
        util::write_u16(&mut cursor, 22).unwrap();
        util::write_u32(&mut cursor, 33).unwrap();
        util::seek(&mut cursor, 0).unwrap();

        let h = TransactionHeader::read(&mut cursor).unwrap();
        assert_eq!(
            h,
            TransactionHeader {
                length: 9999, id: util::p64(1234567890), ndata: 2,
                luser: 11, ldesc: 22, lext: 33,
            });
    }
    
}
