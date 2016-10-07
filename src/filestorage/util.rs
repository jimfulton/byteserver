pub use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
pub use std::io;
pub use std::io::prelude::*;
pub use std::fs::File;

#[macro_export]
macro_rules! io_assert {
    ($cond: expr, $msg: expr ) => (
        if ! ($cond) {
            return io_error($msg)
        }
    )
}

pub type Tid = [u8; 8];
pub type Oid = [u8; 8];
pub type Bytes = Vec<u8>;

pub static TRANSACTION_MARKER: &'static [u8] = b"TTTT";
pub static PADDING_MARKER: &'static [u8] = b"PPPP";

pub static Z64: [u8; 8] = [0u8; 8];
pub fn p64(i: u64) -> [u8; 8] {
    let mut r = [0u8; 8];
    LittleEndian::write_u64(&mut r, i);
    r
}

pub fn io_error<T>(message: &str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::Other, message))
}

pub fn check_magic(reader: &mut io::Read, magic: &[u8]) -> io::Result<()> {
    let mut buf = [0u8; 4];
    try!(reader.read_exact(&mut buf));
    io_assert!(&buf == magic, "bad magic");
    Ok(())
}

pub fn read_sized(reader: &mut io::Read, size: usize) -> io::Result<Vec<u8>> {
    if size > 0 {
        let mut r = vec![0u8; size];
        try!(reader.read_exact(&mut r));
        Ok(r)
    }
    else {
        Ok(vec![0u8; 0])
    }
}


pub fn read_sized16(reader: &mut io::Read) -> io::Result<Vec<u8>> {
    let size = try!(reader.read_u16::<LittleEndian>()) as usize;
    read_sized(reader, size)
}

pub fn read1(reader: &mut io::Read) -> io::Result<u8> {
    let mut r = [0u8];
    try!(reader.read_exact(&mut r));
    Ok(r[0])
}

pub fn read4(reader: &mut io::Read) -> io::Result<[u8; 4]> {
    let mut r = [0u8; 4];
    try!(reader.read_exact(&mut r));
    Ok::<[u8; 4], io::Error>(r)
}

pub fn read8(reader: &mut io::Read) -> io::Result<[u8; 8]> {
    let mut r = [0u8; 8];
    try!(reader.read_exact(&mut r));
    Ok::<[u8; 8], io::Error>(r)
}
