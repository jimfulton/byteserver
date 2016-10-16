extern crate byteserver;

use byteserver::util;
use byteserver::util::*;
use byteserver::errors::*;

enum ClientMessage {
    Locked(Tid),
    Finished(Tid, u64, u64),
    Invalidate(Tid, Vec<Oid>),
}

#[derive(Debug, Clone)]
struct Client {
    name: String,
    send: std::sync::mpsc::Sender<ClientMessage>,
}

impl Client {
    fn new(name: &str) -> (Client, std::sync::mpsc::Receiver<ClientMessage>) {
        let (send, receive) = std::sync::mpsc::channel();
        (Client { name: String::from(name), send: send }, receive)
    }
}

impl PartialEq for Client {
    fn eq(&self, other: &Client) -> bool {
        self.name == other.name
    }
}

impl byteserver::storage::Client for Client {
    fn finished(&self, tid: &Tid, len:u64, size: u64) -> Result<()> {
        self.send.send(ClientMessage::Finished(tid.clone(), len, size))
            .chain_err(|| "")
    }
    fn invalidate(&self, tid: &Tid, oids: &Vec<Oid>) -> Result<()> {
        self.send.send(ClientMessage::Invalidate(
            tid.clone(), oids.clone())).chain_err(|| "")
    }
    fn close(&self) {}
}

#[test]
fn store() {

    let tmpdir = util::test::dir();
    let fs = byteserver::storage::FileStorage::open(
        util::test::test_path(&tmpdir, "data.fs")).unwrap();

    let (client, receive) = Client::new("0");
    let mut clients = vec![Client::new("1"), Client::new("2")];
    fs.add_client(client.clone());
    for &(ref c, _) in clients.iter() {
        fs.add_client(c.clone());
    }

    // First transaction:
    let mut trans = fs.tpc_begin(b"", b"", b"").unwrap();
    trans.save(p64(0), Z64, b"zzzz").unwrap();
    trans.save(p64(1), Z64, b"oooo").unwrap();
    let tx = client.send.clone();
    fs.lock(&trans, Box::new(
        move | id | tx.send(ClientMessage::Locked(id)).unwrap())).unwrap();
    match receive.recv().unwrap() {
        ClientMessage::Locked(tid) => assert_eq!(tid, trans.id),
        _ => panic!("bad message"),
    }
    trans.locked().unwrap();
    let conflicts = fs.stage(&mut trans).unwrap();
    assert_eq!(conflicts.len(), 0);
    
    fs.tpc_finish(&trans.id, client.clone()).unwrap();
    let tid0 = match receive.recv().unwrap() {
        ClientMessage::Finished(tid, len, size) => {
            assert_eq!(len, 2);
            assert_eq!(size, 4216);
            tid
        },
        _ => panic!("bad message"),
    };
    assert!(receive.try_recv().is_err());
    for &(_, ref receive) in clients.iter() {
        match receive.recv().unwrap() {
            ClientMessage::Invalidate(tid, oids) => {
                assert_eq!(tid, tid0);
                assert_eq!(oids, vec![p64(0), p64(1)]);
            },
            _ => panic!("bad message"),
        }
        assert!(receive.try_recv().is_err());
    }
    assert_eq!(fs.last_transaction(), tid0);

    use byteserver::storage::LoadBeforeResult::*;
    
    let r = fs.load_before(&p64(1), &byteserver::tid::next(&tid0)).unwrap();
    match r {
        Loaded(data, tid, None) => {
            assert_eq!(data, b"oooo".to_vec());
            assert_eq!(tid, tid0);
        },
        _ => panic!("unexpeted result {:?}", r),
    }

    // Drop one of the clients.  We should see the storage client
    // count drop after it tries to send to them.
    clients.pop();

    // Second, conflict and then success:
    let mut trans = fs.tpc_begin(b"", b"", b"").unwrap();
    trans.save(p64(1), Z64, b"ooo1").unwrap();
    let tx = client.send.clone();
    fs.lock(&trans, Box::new(
        move | id | tx.send(ClientMessage::Locked(id)).unwrap())).unwrap();
    match receive.recv().unwrap() {
        ClientMessage::Locked(tid) => assert_eq!(tid, trans.id),
        _ => panic!("bad message"),
    }
    trans.locked().unwrap();
    let conflicts = fs.stage(&mut trans).unwrap();

    use byteserver::storage::Conflict;
    assert_eq!(
        conflicts,
        vec![Conflict { oid: p64(1), serials: (Z64, tid0),
                        data: b"ooo1".to_vec() }]);

    trans.save(p64(1), tid0, b"ooo2").unwrap();
    let tx = client.send.clone();
    fs.lock(&trans, Box::new(
        move | id | tx.send(ClientMessage::Locked(id)).unwrap())).unwrap();
    match receive.recv().unwrap() {
        ClientMessage::Locked(tid) => assert_eq!(tid, trans.id),
        _ => panic!("bad message"),
    }
    trans.locked().unwrap();
    let conflicts = fs.stage(&mut trans).unwrap();
    assert_eq!(conflicts.len(), 0);
    
    fs.tpc_finish(&trans.id, client.clone()).unwrap();
    let tid1 = match receive.recv().unwrap() {
        ClientMessage::Finished(tid, len, size) => {
            assert_eq!(len, 2);
            assert_eq!(size, 4296);
            tid
        },
        _ => panic!("bad message"),
    };
    assert!(receive.try_recv().is_err());
    for &(_, ref receive) in clients.iter() {
        match receive.recv().unwrap() {
            ClientMessage::Invalidate(tid, oids) => {
                assert_eq!(tid, tid1);
                assert_eq!(oids, vec![p64(1)]);
            },
            _ => panic!("bad message"),
        }
        assert!(receive.try_recv().is_err());
    }
    assert_eq!(fs.last_transaction(), tid1);

    assert_eq!(fs.client_count(), 2);

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

#[test]
fn abort() {

    let tmpdir = util::test::dir();
    let fs = byteserver::storage::FileStorage::open(
        util::test::test_path(&tmpdir, "data.fs")).unwrap();

    let (client, receive) = Client::new("0");
    fs.add_client(client.clone());
 
    let mut trans = fs.tpc_begin(b"", b"", b"").unwrap();
    trans.save(p64(0), Z64, b"zzzz").unwrap();
    let tx = client.send.clone();
    fs.lock(&trans, Box::new(
        move | id | tx.send(ClientMessage::Locked(id)).unwrap())).unwrap();
    match receive.recv().unwrap() {
        ClientMessage::Locked(tid) => assert_eq!(tid, trans.id),
        _ => panic!("bad message"),
    }
    trans.locked().unwrap();

    // Abort releases the lock, so we can start over:
    fs.tpc_abort(&trans.id);

    let mut trans = fs.tpc_begin(b"", b"", b"").unwrap();
    trans.save(p64(0), Z64, b"zzzz").unwrap();
    let tx = client.send.clone();
    fs.lock(&trans, Box::new(
        move | id | tx.send(ClientMessage::Locked(id)).unwrap())).unwrap();
    match receive.recv().unwrap() {
        ClientMessage::Locked(tid) => assert_eq!(tid, trans.id),
        _ => panic!("bad message"),
    }
    trans.locked().unwrap();    
    let conflicts = fs.stage(&mut trans).unwrap();
    assert_eq!(conflicts.len(), 0);
    fs.tpc_abort(&trans.id);

    // Abort releases locks *and* prevents the transaction from committing.

    // We'll go again, which would fail if the previous attempts had
    // committed:
    
    let mut trans = fs.tpc_begin(b"", b"", b"").unwrap();
    trans.save(p64(0), Z64, b"zzzz").unwrap();
    let tx = client.send.clone();
    fs.lock(&trans, Box::new(
        move | id | tx.send(ClientMessage::Locked(id)).unwrap())).unwrap();
    match receive.recv().unwrap() {
        ClientMessage::Locked(tid) => assert_eq!(tid, trans.id),
        _ => panic!("bad message"),
    }
    trans.locked().unwrap();    
    let conflicts = fs.stage(&mut trans).unwrap();
    assert_eq!(conflicts.len(), 0);
    fs.tpc_finish(&trans.id, client.clone()).unwrap();
    match receive.recv().unwrap() {
        ClientMessage::Finished(_, _, _) => {
        },
        _ => panic!("bad message"),
    }
    assert!(receive.try_recv().is_err());
}
