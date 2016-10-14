extern crate byteserver;

use byteserver::util;
use byteserver::util::*;

#[test]
fn store() {

    let tmpdir = util::test::dir();
    let fs = byteserver::storage::FileStorage::open(
        util::test::test_path(&tmpdir, "data.fs")).unwrap();
    let (send, receive) = std::sync::mpsc::channel();

    // First transaction:
    let mut trans = fs.tpc_begin(b"", b"", b"").unwrap();
    trans.save(p64(0), Z64, b"zzzz").unwrap();
    trans.save(p64(1), Z64, b"oooo").unwrap();
    let tx = send.clone();
    fs.lock(&trans, Box::new(move | id | tx.send(id).unwrap())).unwrap();
    assert_eq!(receive.recv().unwrap(), trans.id);
    trans.locked().unwrap();
    let conflicts = fs.stage(&mut trans).unwrap();
    assert_eq!(conflicts.len(), 0);
    
    let tx = send.clone();
    fs.tpc_finish(&trans.id, Box::new(
        move | tid | tx.send(tid).unwrap())).unwrap();
    let tid0 = receive.recv().unwrap();

    use byteserver::storage::LoadBeforeResult::*;
    
    let r = fs.load_before(&p64(1), &byteserver::tid::next(&tid0)).unwrap();
    match r {
        Loaded(data, tid, None) => {
            assert_eq!(data, b"oooo".to_vec());
            assert_eq!(tid, tid0);
        },
        _ => panic!("unexpeted result {:?}", r),
    }

    // Second, conflict and then success:
    let mut trans = fs.tpc_begin(b"", b"", b"").unwrap();
    trans.save(p64(1), Z64, b"ooo1").unwrap();
    let tx = send.clone();
    fs.lock(&trans, Box::new(move | id | tx.send(id).unwrap())).unwrap();
    assert_eq!(receive.recv().unwrap(), trans.id);
    trans.locked().unwrap();
    let conflicts = fs.stage(&mut trans).unwrap();

    use byteserver::storage::Conflict;
    assert_eq!(
        conflicts,
        vec![Conflict { oid: p64(1), serials: (Z64, tid0),
                        data: b"ooo1".to_vec() }]);

    trans.save(p64(1), tid0, b"ooo2").unwrap();
    let tx = send.clone();
    fs.lock(&trans, Box::new(move | id | tx.send(id).unwrap())).unwrap();
    assert_eq!(receive.recv().unwrap(), trans.id);
    trans.locked().unwrap();
    let conflicts = fs.stage(&mut trans).unwrap();
    assert_eq!(conflicts.len(), 0);
    
    let tx = send.clone();
    fs.tpc_finish(&trans.id, Box::new(
        move | tid | tx.send(tid).unwrap())).unwrap();
    let tid1 = receive.recv().unwrap();

    let r = fs.load_before(&p64(1), &tid1).unwrap();
    match r {
        Loaded(data, tid, Some(end)) => {
            assert_eq!(data, b"oooo".to_vec());
            assert_eq!(tid, tid0);
            assert_eq!(end, tid1);
        },
        _ => panic!("unexpeted result {:?}", r),
    }

    let r = fs.load_before(&p64(1), &byteserver::tid::next(&tid1)).unwrap();
    match r {
        Loaded(data, tid, None) => {
            assert_eq!(data, b"ooo2".to_vec());
            assert_eq!(tid, tid1);
        },
        _ => panic!("unexpeted result {:?}", r),
    }

}
