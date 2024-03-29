// Test of the byteserver reader process
use std::io::prelude::*;

#[macro_use]
extern crate byteserver;

use std::collections::BTreeMap;

use anyhow::Context;
use byteorder::{ByteOrder, BigEndian};
use serde::bytes::ByteBuf;

use byteserver::msg;
use byteserver::msgmacros::*;
use byteserver::util;
use byteserver::reader;
use byteserver::writer;
use byteserver::storage;
use byteserver::tid;

fn unsize(mut v: Vec<u8>) -> Vec<u8> {
    assert_eq!(BigEndian::read_u32(&v), v.len() as u32 - 4);
    v.split_off(4)
}

#[test]
fn basic() {
    let (reader, mut writer) = pipe::pipe();
    let (tx, rx) = std::sync::mpsc::channel();

    let tdir = byteserver::util::test::dir();
    let path = byteserver::util::test::test_path(&tdir, "data.fs");

    storage::testing::make_sample(
        &path,
        vec![vec![(util::Z64, b"000")],
             vec![(util::Z64, b"111"), (util::p64(3), b"ooo")],
        ],
    ).unwrap();
    let fs = std::sync::Arc::new(
        storage::FileStorage::<writer::Client>::open(path).unwrap());
    let read_fs = fs.clone();

    std::thread::spawn(
        move || reader::reader(read_fs, reader, tx).unwrap()
    );

    // handshake
    writer.write_all(&msg::size_vec(b"M5".to_vec())).unwrap();
    // register
    writer.write_all(&sencode!((1, "register", ("1", true))).unwrap()).unwrap();
    // This generates a response directly
    match rx.recv().unwrap() {
        msg::Zeo::Raw(r) => {
            let r = unsize(r);
            let (id, code, tid): (u64, String, ByteBuf) =
                decode!(&mut (&r as &[u8]),
                        "decoding register response").unwrap();
            assert_eq!(id, 1); assert_eq!(&code, "R");
            assert_eq!(util::read8(&mut (&*tid)).unwrap(), fs.last_transaction());
        }, _ => panic!("invalid message")
    }
    // get_info(), mostly punt for now:
    writer.write_all(&sencode!((2, "get_info", ())).unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::Raw(r) => {
            let r = unsize(r);
            let (id, code, info): (u64, String, BTreeMap<String, u64>) =
                decode!(&mut (&r as &[u8]),
                        "decoding get_info response").unwrap();
            assert_eq!(id, 2); assert_eq!(&code, "R");
            assert_eq!(info, BTreeMap::new());
        }, _ => panic!("invalid message")
    }
    // loadBefore
    // current:
    let now = tid::next(&tid::now_tid());
    writer.write_all(
        &sencode!((3, "loadBefore", (util::Z64, now))).unwrap()).unwrap();
    let tid1 = match rx.recv().unwrap() {
        msg::Zeo::Raw(r) => {
            let r = unsize(r);
            let (id, code, (data, tid, end)): (
                u64, String, (ByteBuf, ByteBuf, Option<ByteBuf>)) =
                decode!(&mut (&r as &[u8]),
                        "decoding loadBefore response").unwrap();
            assert_eq!(id, 3); assert_eq!(&code, "R");
            assert_eq!(&*data, b"111");
            assert!(end.is_none());
            util::read8(&mut &*tid).unwrap()
        }, _ => panic!("invalid message")
    };
    // previous
    writer.write_all(
        &sencode!((3, "loadBefore", (util::Z64, tid1))).unwrap()).unwrap();
    let tid0 = match rx.recv().unwrap() {
        msg::Zeo::Raw(r) => {
            let r = unsize(r);
            let (id, code, (data, tid, end)): (
                u64, String, (ByteBuf, ByteBuf, Option<ByteBuf>)) =
                decode!(&mut (&r as &[u8]),
                        "decoding loadBefore response").unwrap();
            assert_eq!(id, 3); assert_eq!(&code, "R");
            assert_eq!(&*data, b"000");
            assert_eq!(util::read8(&mut &*end.unwrap()).unwrap(), tid1);
            util::read8(&mut &*tid).unwrap()
        }, _ => panic!("invalid message")
    };
    // pre creation
    writer.write_all(
        &sencode!((3, "loadBefore", (util::Z64, tid0))).unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::Raw(r) => {
            let r = unsize(r);
            let (id, code, n): (u64, String, Option<u32>) =
                decode!(&mut (&r as &[u8]),
                        "decoding loadBefore response").unwrap();
            assert_eq!(id, 3); assert_eq!(&code, "R");
            assert!(n.is_none());
        }, _ => panic!("invalid message")
    }
    // Error
    writer.write_all(
        &sencode!((3, "loadBefore", (util::p64(9), tid0))).unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::Raw(r) => {
            let r = unsize(r);
            let (id, code, (ename, (oid,))): (
                u64, String, (String, (ByteBuf,))) =
                decode!(&mut (&r as &[u8]),
                        "decoding loadBefore response").unwrap();
            assert_eq!(id, 3); assert_eq!(&code, "E");
            assert_eq!(ename, "ZODB.POSException.POSKeyError");
            assert_eq!(&*oid, &util::p64(9))
        }, _ => panic!("invalid message")
    }

    // Ping
    writer.write_all(&sencode!((4, "ping", ())).unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::Raw(r) => {
            let r = unsize(r);
            let (id, code, r): (u64, String, Option<u32>) =
                decode!(&mut (&r as &[u8]),
                        "decoding ping response").unwrap();
            assert_eq!(id, 4); assert_eq!(&code, "R");
            assert!(r.is_none());
        }, _ => panic!("invalid message")
    }

    // new_oids:
    writer.write_all(&sencode!((4, "new_oids", ())).unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::Raw(r) => {
            let r = unsize(r);
            let (id, code, oids): (u64, String, Vec<ByteBuf>) =
                decode!(&mut (&r as &[u8]),
                        format!("decoding new_oids response {:?}", r)).unwrap();
            assert_eq!(id, 4); assert_eq!(&code, "R");
            assert_eq!(
                oids,
                (4..104)
                    .map(| oid | ByteBuf::from(util::p64(oid).to_vec()))
                    .collect::<Vec<ByteBuf>>()
            )
        }, _ => panic!("invalid message")
    }
    
    // Requests that deal with transactions are merely forwarded:
    writer.write_all(
        &sencode!((0, "tpc_begin", (42, b"u", b"d", b"e", msg::NIL, b" ")))
            .unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::TpcBegin(42, user, desc, ext) => {
            assert_eq!((user, desc, ext),
                       (b"u".to_vec(), b"d".to_vec(), b"e".to_vec()));
        }, _ => panic!("invalid message")
    }
    writer.write_all(
        &sencode!((0, "storea", (util::Z64, fs.last_transaction(), b"111", 42)))
                  .unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::Storea(oid, serial, data, 42) => {
            assert_eq!((oid, serial, data),
                       (util::Z64, fs.last_transaction(), b"111".to_vec()));
        }, _ => panic!("invalid message")
    }
    writer.write_all(
        &sencode!((4, "vote", (42,))).unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::Vote(4, 42) => {
        }, _ => panic!("invalid message")
    }
    writer.write_all(
        &sencode!((5, "tpc_finish", (42,))).unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::TpcFinish(5, 42) => {
        }, _ => panic!("invalid message")
    }
    writer.write_all(
        &sencode!((5, "tpc_abort", (42,))).unwrap()).unwrap();
    match rx.recv().unwrap() {
        msg::Zeo::TpcAbort(5, 42) => {
        }, _ => panic!("invalid message")
    }
}
