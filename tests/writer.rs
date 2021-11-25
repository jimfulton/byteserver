// Test the writer half of the server


extern crate byteorder;
extern crate serde;

#[macro_use]
extern crate byteserver;
extern crate pipe;

use std::collections::BTreeMap;

use anyhow::Context;
use serde::bytes::ByteBuf;

use byteserver::msg::*;
use byteserver::util::*;
use byteserver::writer;
use byteserver::storage;

#[test]
fn basic() {
    let (reader, writer) = pipe::pipe();
    let (tx, rx) = std::sync::mpsc::channel();

    let tdir = byteserver::util::test::dir();
    let path = byteserver::util::test::test_path(&tdir, "data.fs");

    storage::testing::make_sample(
        &path, vec![vec![(Z64, b"000")], vec![(Z64, b"111")]]).unwrap();
    let fs = Arc::new(
        storage::FileStorage::<writer::Client>::open(path).unwrap());

    let client = writer::Client::new("test".to_string(), tx.clone());
    fs.add_client(client.clone());
    let write_fs = fs.clone();
    let write_client = client.clone();
    std::thread::spawn(
        move || writer::writer(write_fs, writer, rx, write_client).unwrap());

    let mut reader = ZeoIter::new(reader);

    // Handshake:
    assert_eq!(&reader.next_vec().unwrap(), b"M5");

    // Lets write some data:
    tx.send(Zeo::TpcBegin(42, b"u".to_vec(), b"d".to_vec(), b"{}".to_vec()))
        .unwrap();
    tx.send(Zeo::Storea(p64(1), Z64, b"ooo".to_vec(), 42)).unwrap();
    tx.send(Zeo::Vote(11, 42)).unwrap();

    // We get back any conflicts:
    let (msgid, flag, conflicts): (
        i64, String, Vec<BTreeMap<String, ByteBuf>>) =
        decode!(&mut (&reader.next_vec().unwrap() as &[u8]),
                "decoding conflicts").unwrap();
    assert_eq!((msgid, &flag as &str), (11, "R"));
    // There weren't any:
    assert_eq!(conflicts.len(), 0);

    // And we finish, getting back a tid and info:
    tx.send(Zeo::TpcFinish(12, 42)).unwrap();
    let (msgid, flag, tid): (i64, String, ByteBuf) =
        decode!(&mut (&reader.next_vec().unwrap() as &[u8]),
                "decoding finish response").unwrap();
    assert_eq!((msgid, &flag as &str), (12, "R"));
    assert_eq!(tid.len(), 8);
    let (msgid, method, (info,)): (i64, String, (BTreeMap<String, u64>,)) =
        decode!(&mut (&reader.next_vec().unwrap() as &[u8]),
                "decoding info").unwrap();
    assert_eq!((msgid, &method as &str), (0, "info"));
    assert_eq!(info, {
        let mut map = BTreeMap::new();
        map.insert("length".to_string(), 2);
        map.insert("size".to_string(), 4337);
        map
    });
    
    if let storage::LoadBeforeResult::Loaded(data, ltid, end) =
        fs.load_before(&p64(1), storage::testing::MAXTID).unwrap() {
            assert_eq!(&ltid, &*tid);
            assert_eq!(&data, b"ooo");
            assert!(end.is_none());
        }
    else { panic!("Couldn't load") }

    // If data are updated not by the client, we'll be notified:
    let (tx2, _) = std::sync::mpsc::channel();
    let client2 = writer::Client::new("test2".to_string(), tx2.clone());
    storage::testing::add_data(&fs, &client2, vec![vec![(p64(3), b"ttt")]])
        .context("adding data").unwrap();
    let (msgid, method, (itid, oids)): (i64, String, (ByteBuf, Vec<ByteBuf>)) = 
        decode!(&mut (&reader.next_vec().unwrap() as &[u8]),
                "decoding invalidations").unwrap();
    assert_eq!((msgid, &method as &str), (0, "invalidateTransaction"));
    assert_eq!(itid.len(), 8);
    assert!(itid > tid);
    assert_eq!(oids, vec![ByteBuf::from(p64(3).to_vec())]);
}
