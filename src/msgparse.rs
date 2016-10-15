use std;

use rmp;
use rmp_serde;
use serde::bytes::ByteBuf;
use serde::Deserialize;

use byteorder::BigEndian;

use errors::*;
use util::*;

macro_rules! decode {
    ($data: expr) => (
        {
            let mut deserializer = rmp_serde::Deserializer::new($data);
            Deserialize::deserialize(&mut deserializer).chain_err(|| "decode")
        }
    )
}

macro_rules! args {
    ($types: expr) => (
        {
            let args: ($types) = try!(decode!(&mut reader));
            args
        }
    )
}

#[derive(Debug, PartialEq)]
pub enum Zeo {
    Error(i64, &'static str, &'static str),
    Raw(Vec<u8>),
    End,
    Register(i64, String, bool),
    LoadBefore(i64, Oid, Tid),
    Finished(Tid, u64, u64),
    Invalidate(Tid, Vec<Oid>),
}

pub struct ZeoIter<T: io::Read> {
    reader: T,
    buf: [u8; 1<<16],
    input: Vec<u8>,
}

static HEARTBEAT_PREFIX: [u8; 2] = [147, 255];

impl<T: io::Read> ZeoIter<T> {

    pub fn new(reader: T) -> ZeoIter<T> {
        ZeoIter { reader: reader, buf: [0u8; 1<<16], input: vec![] }
    }

    fn read_want(&mut self, want: usize) -> Result<bool> {
        while self.input.len() < want {
            let n = try!(self.reader.read(&mut self.buf)
                         .chain_err(|| "reading"));
            if n > 0 {
                self.input.extend_from_slice(&self.buf[..n]);
            }
            else {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn advance(&mut self) -> Result<usize> {
        Ok(
            if try!(self.read_want(4)) { 0 }
            else {
                let want = (BigEndian::read_u32(&self.input) + 4) as usize; 
                if try!(self.read_want(want)) { 0 }
                else { want }
            }
        )
    }

    pub fn next_vec(&mut self) -> Result<Vec<u8>> {
        let want = try!(self.advance());
        let mut data = self.input.split_off(want as usize);
        std::mem::swap(&mut data, &mut self.input);
        Ok(data.split_off(4))
    }

    pub fn next(&mut self) -> Result<Zeo> {
        let want = try!(self.advance());
        if want == 0 {
            return Ok(Zeo::End);
        }
        let mut data = self.input.split_off(want as usize);
        std::mem::swap(&mut data, &mut self.input);
        
        if data[4..6] == HEARTBEAT_PREFIX {
            return self.next()    // skip heartbeats
        }
        let mut reader = io::Cursor::new(data.split_off(4));
        parse_message(&mut reader)
    }
    
}

fn pre_parse(mut reader: &mut io::Read)
             -> Result<(i64, String)> {
    let array_size = try!(rmp::decode::read_array_size(&mut reader)
                          .chain_err(|| "get mess size"));
    if array_size != 3 {
        return Err(format!("Bad array size {}", array_size).into());
    }
    let id: i64 = try!(decode!(&mut reader).chain_err(|| "reading id"));
    let method: String = try!(decode!(&mut reader));
    Ok((id, method))
}

fn parse_message(mut reader: &mut io::Read) -> Result<Zeo> {
    let (id, method) = try!(pre_parse(&mut reader));

    Ok(match method.as_ref() {
        "register" => {
            let (storage, read_only): (String, bool) =
                try!(decode!(&mut reader));
            Zeo::Register(id, storage, read_only)
        },
        "loadBefore" => {
            let (oid, before): (ByteBuf, ByteBuf) =
                try!(decode!(&mut reader));
            let oid = try!(read8(&mut (&*oid)).chain_err(|| "loadBefore oid"));
            let before = try!(read8(&mut (&*before))
                              .chain_err(|| "loadBefore before"));
            Zeo::LoadBefore(id, oid, before)
        },
        _ => return Err("bad method".into())
    })
}


// ======================================================================

#[cfg(test)]
mod tests {

    use super::*;
    use std::io;

    #[test]
    fn works() {
        let mut buf: Vec<u8> = vec![];

        // Handshake, M5
        buf.extend_from_slice(b"\x00\x00\x00\x02M5");
        // (1, 'register', '1', false)
        buf.extend_from_slice(
            b"\x00\x00\x00\x0f\x93\x01\xa8register\x92\xa11\xc2");
        // (2, 'loadBefore', (b"\0\0\0\0\0\0\0\0", b"\1\1\1\1\1\1\1\1"))
        buf.extend_from_slice(
            &[0, 0, 0, 34, 147, 2, 170, 108, 111, 97, 100, 66, 101,
              102, 111, 114, 101, 146, 196, 8, 0, 0, 0, 0, 0, 0, 0, 0,
              196, 8, 1, 1, 1, 1, 1, 1, 1, 1]);
        let reader = io::Cursor::new(buf);

        let mut it = ZeoIter::new(reader);
        assert_eq!(&it.next_vec().unwrap(), b"M5");
        match it.next().unwrap() {
            Zeo::Register(1, storage, false) => {
                assert_eq!(&storage, "1");
            },
            _ => panic!("bad match")
        }
        match it.next().unwrap() {
            Zeo::LoadBefore(2, oid, tid) => {
                assert_eq!(oid, [0u8; 8]);
                assert_eq!(tid, [1u8; 8]);
            },
            _ => panic!("bad match")
        }
    }
}
