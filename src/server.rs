use std;

use storage;
use transaction;
use errors::*;
use util::*;
use msg::*;

macro_rules! response {
    ($id: expr, $data: expr) => (
        try!(sencode!(($id, "R", ($data))))
    )
}

macro_rules! respond {
    ($sender: expr, $id: expr, $data: expr) => (
        try!($sender.send(Zeo::Raw(response!($id, $data)))
             .chain_err(|| "send response"))
    )
}

macro_rules! error_response {
    ($id: expr, $data: expr) => (
        try!(sencode!(($id, "E", ($data))))
    )
}

macro_rules! error_respond {
    ($sender: expr, $id: expr, $data: expr) => (
        try!($sender.send(Zeo::Raw(error_response!($id, $data)))
             .chain_err(|| "send error response"))
    )
}

pub const NIL: Option<u32> = None;

pub fn reader<C: storage::Client, R: io::Read>(
    fs: Arc<storage::FileStorage<C>>,
    reader: R,
    sender: std::sync::mpsc::Sender<Zeo>)
    -> Result<()> {

    let mut it = ZeoIter::new(reader);

    // handshake
    if try!(it.next_vec()) != b"M5".to_vec() {
        return Err("Bad handshake".into())
    }

    // register(storage_id, read_only)
    loop {
        match try!(it.next()) {
            Zeo::Register(id, storage, read_only) => {
                if &storage != "1" {
                    sender.send(Zeo::Error(
                        id, "builtins.ValueError", "Invalid storage"));
                }
                respond!(sender, id, fs.last_transaction());
                break;          // onward
            },
            Zeo::End => {
                sender.send(Zeo::End);
                return Ok(())
            },
            _ => return Err("bad method".into())
        }
    }

    // Main loop. We spend most of our time here.
    loop {
        let message = try!(it.next());
        match message {
            Zeo::LoadBefore(id, oid, before) => {
                use storage::LoadBeforeResult::*;
                match try!(fs.load_before(&oid, &before)) {
                    Loaded(data, tid, Some(end)) => {
                            respond!(sender, id, (data, tid, end));
                        },
                    Loaded(data, tid, None) => {
                        respond!(sender, id, (data, tid, NIL));
                    },
                    NoneBefore => {
                        respond!(sender, id, NIL);
                    },
                    PosKeyError => {
                        error_respond!(
                            sender, id,
                            ("ZODB.POSException.POSKeyError", (oid,)));
                    },
                }
            },
            Zeo::Ping(id) => {
                respond!(sender, id, NIL);
            },
            Zeo::GetInfo(id) => {
                respond!(sender, id,
                         std::collections::BTreeMap::<String, i64>::new())
            },
            Zeo::TpcBegin(_, _, _, _) | Zeo::Storea(_, _, _, _) |
            Zeo::Vote(_, _) | Zeo::TpcFinish(_, _) |  Zeo::TpcAbort(_, _)
                =>  try!(sender.send(message)
                         .chain_err(|| "send error")), // Forward these
            Zeo::End => {
                sender.send(Zeo::End);
                return Ok(())
            },
            _ => return Err("bad method".into())
        }            
    }
}

fn writer<C: storage::Client>(
    fs: Arc<storage::FileStorage<C>>,
    mut stream: std::net::TcpStream,
    receiver: std::sync::mpsc::Receiver<Zeo>,
    client: Client)
    -> Result<()> {

    use std::collections::HashMap;
    let mut transactions: HashMap<u64, transaction::Transaction> =
        HashMap::new();

    for zeo in receiver.iter() {
        match zeo {
            Zeo::Error(id, name, message) => {
                try!(stream.write_all(&try!(sencode!(
                    (id, "E", (name, (message,)))
                ))).chain_err(|| "stream write"));
            },
            Zeo::End => break,
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct Client {
    name: String,
    send: std::sync::mpsc::Sender<Zeo>,
}

impl PartialEq for Client {
    fn eq(&self, other: &Client) -> bool {
        self.name == other.name
    }
}

impl storage::Client for Client {
    fn finished(&self, tid: &Tid, len: u64, size: u64) -> Result<()>  {
        self.send.send(Zeo::Finished(tid.clone(), len, size)).chain_err(|| "")
    }
    fn invalidate(&self, tid: &Tid, oids: &Vec<Oid>) -> Result<()>  {
        self.send.send(Zeo::Invalidate(
            tid.clone(), oids.clone())).chain_err(|| "")
    }
    fn close(&self) {}
}


fn main() {

    // To do, options :)
    let fs = Arc::new(
        storage::FileStorage::<Client>::open(String::from("data.fs")).unwrap());
    
    let listener = std::net::TcpListener::bind("127.0.0.1:8080").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Accepted {:?}", stream);
                let (send, receive) = std::sync::mpsc::channel();
                let client = Client {
                    name: format!("{:?}", (stream.peer_addr())),
                    send: send.clone(),
                };
                fs.add_client(client.clone());

                let read_fs = fs.clone();
                let read_stream = stream.try_clone().unwrap();
                
                std::thread::spawn(
                    move || reader(read_fs, read_stream, send).unwrap());

                let write_fs = fs.clone();
                std::thread::spawn(
                    move ||
                        writer(write_fs, stream, receive, client).unwrap());
            },
            Err(e) => { println!("WTF {}", e) }
        }
    }
}
