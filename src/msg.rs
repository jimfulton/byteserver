
use byteorder::{BigEndian, ByteOrder};

use serde::bytes::ByteBuf;

use anyhow::{anyhow, Context, Result};

use crate::util;
use crate::msgmacros::*;

pub fn size_vec(mut v: Vec<u8>) -> Vec<u8> {
    let l = v.len();
    for i in 0..4 {
        v.insert(0, 0);
    }
    BigEndian::write_u32(&mut v, l as u32);
    v
}

pub const NIL: Option<u32> = None;

pub fn bytes(data: &[u8]) -> serde::bytes::Bytes {
    serde::bytes::Bytes::new(data)
}

#[derive(Debug, PartialEq)]
pub enum Zeo {
    Raw(Vec<u8>),
    End,

    Register(i64, String, bool),
    LoadBefore(i64, util::Oid, util::Tid),
    GetInfo(i64),
    NewOids(i64),
    TpcBegin(u64, util::Bytes, util::Bytes, util::Bytes),
    Storea(util::Oid, util::Tid, util::Bytes, u64),
    Vote(i64, u64),
    TpcFinish(i64, u64),
    TpcAbort(i64, u64),
    Ping(i64),

    Locked(i64, u64),

    Finished(i64, util::Tid, u64, u64),
    Invalidate(util::Tid, Vec<util::Oid>),
}

pub struct ZeoIter<T: std::io::Read> {
    reader: T,
    buf: [u8; 1<<16],
    input: Vec<u8>,
}

static HEARTBEAT_PREFIX: [u8; 2] = [147, 255];

impl<T: std::io::Read> ZeoIter<T> {

    pub fn new(reader: T) -> ZeoIter<T> {
        ZeoIter { reader: reader, buf: [0u8; 1<<16], input: vec![] }
    }

    fn read_want(&mut self, want: usize) -> Result<bool> {
        while self.input.len() < want {
            let n = self.reader.read(&mut self.buf).context("reading")?;
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
            if self.read_want(4)? { 0 }
            else {
                let want = (BigEndian::read_u32(&self.input) + 4) as usize;
                if self.read_want(want)? { 0 }
                else { want }
            }
        )
    }

    pub fn next_vec(&mut self) -> Result<Vec<u8>> {
        let want = self.advance()?;
        let mut data = self.input.split_off(want as usize);
        std::mem::swap(&mut data, &mut self.input);
        Ok(data.split_off(4))
    }

    pub fn next(&mut self) -> Result<Zeo> {
        let want = self.advance()?;
        if want == 0 {
            return Ok(Zeo::End);
        }
        let mut data = self.input.split_off(want as usize);
        std::mem::swap(&mut data, &mut self.input);

        if data[4..6] == HEARTBEAT_PREFIX {
            return self.next()    // skip heartbeats
        }
        //println!("Read vec {:?}", &data[4..]);
        let mut reader = std::io::Cursor::new(data.split_off(4));
        parse_message(&mut reader)
    }

}

fn pre_parse(mut reader: &mut dyn std::io::Read)
             -> Result<(i64, String)> {
    let array_size =
        rmp::decode::read_array_size(&mut reader).context("get mess size")?;
    if array_size != 3 {
        return Err(anyhow!("Invalid message size. Expect 3, got {}", array_size))?;
    }
    let id: i64 = decode!(&mut reader, "decoding message id")?;
    let method: String = decode!(&mut reader, "decoding message name")?;
    Ok((id, method))
}

fn parse_message(mut reader: &mut dyn std::io::Read) -> Result<Zeo> {
    let (id, method) = pre_parse(&mut reader)?;

    Ok(match method.as_ref() {
        "loadBefore" => {
            let (oid, before): (ByteBuf, ByteBuf) =
                decode!(&mut reader, "decoding loadBefore oid")?;
            let oid = util::read8(&mut (&*oid)).context("loadBefore oid")?;
            let before =
                util::read8(&mut (&*before))
                .context("loadBefore before")?;
            Zeo::LoadBefore(id, oid, before)
        },
        "ping" => Zeo::Ping(id),
        "tpc_begin" => {
            let (txn, user, desc, ext, _, _): (
                u64, ByteBuf, ByteBuf, ByteBuf, Option<ByteBuf>, ByteBuf) =
                decode!(&mut reader, "decoding tpc_begin")?;
            Zeo::TpcBegin(txn, user.to_vec(), desc.to_vec(), ext.to_vec())
        },
        "storea" => {
            let (oid, committed, data, txn): (ByteBuf, ByteBuf, ByteBuf, u64) =
                decode!(&mut reader, "decoding storea")?;
            let oid = util::read8(&mut (&*oid)).context("storea oid")?;
            let committed =
                util::read8(&mut (&*committed))
                .context("storea committed")?;
            Zeo::Storea(oid, committed, data.to_vec(), txn)
        },
        "vote" => {
            let (txn,): (u64,) = decode!(&mut reader, "decoding vote")?;
            Zeo::Vote(id, txn)
        },
        "tpc_finish" => {
            let (txn,): (u64,) = decode!(&mut reader, "decoding tpc_finish")?;
            Zeo::TpcFinish(id, txn)
        },
        "tpc_abort" => {
            let (txn,): (u64,) = decode!(&mut reader, "decoding tpc_abort")?;
            Zeo::TpcAbort(id, txn)
        },
        "new_oids" => Zeo::NewOids(id),
        "get_info" => Zeo::GetInfo(id),
        "register" => {
            let (storage, read_only): (String, bool) =
                decode!(&mut reader, "decoding register")?;
            Zeo::Register(id, storage, read_only)
        },
        _ => return Err(anyhow!("bad method {}", method))?
    })
}


// ======================================================================

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parsing() {
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
        let reader = std::io::Cursor::new(buf);

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

    #[test]
    fn test_size_vec() {
        assert_eq!(size_vec(vec![1, 2, 3]), vec![0, 0, 0, 3, 1, 2, 3]);
    }

    #[test]
    fn test_sencode() {
        let v = sencode!((1u64, "R", 42)).unwrap();
        assert_eq!(v, vec![0, 0, 0, 5, 147, 1, 161, 82, 42]);
    }

}
