use std;

use storage;
use errors::*;
use util::*;
use reader;
use writer;

fn main() {

    // TODO, options :)
    let fs = Arc::new(
        storage::FileStorage::<writer::Client>::open(
            String::from("data.fs")).unwrap());
    
    let listener = std::net::TcpListener::bind("127.0.0.1:8080").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Accepted {:?}", stream);
                let (send, receive) = std::sync::mpsc::channel();
                let client = writer::Client::new(
                    stream.peer_addr().unwrap().to_string(), send.clone());
                fs.add_client(client.clone());

                let read_fs = fs.clone();
                let read_stream = stream.try_clone().unwrap();
                
                std::thread::spawn(
                    move ||
                        reader::reader(read_fs, read_stream, send).unwrap());

                let write_fs = fs.clone();
                std::thread::spawn(
                    move ||
                        writer::writer(
                            write_fs, stream, receive, client).unwrap());
            },
            Err(e) => { println!("WTF {}", e) }
        }
    }
}
