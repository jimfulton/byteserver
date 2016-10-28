extern crate byteserver;

fn main() {

    // TODO, options :)
    let fs = std::sync::Arc::new(
        byteserver::storage::FileStorage::<byteserver::writer::Client>::open(
            String::from("data.fs")).unwrap());
    
    let listener = std::net::TcpListener::bind("0.0.0.0:8080").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                stream.set_nodelay(true).unwrap();
                println!("Accepted {:?} {}", stream, stream.nodelay().unwrap());
                let (send, receive) = std::sync::mpsc::channel();

                let client = byteserver::writer::Client::new(
                    stream.peer_addr().unwrap().to_string(), send.clone());
                fs.add_client(client.clone());

                let read_fs = fs.clone();
                let read_stream = stream.try_clone().unwrap();
                std::thread::spawn(
                    move ||
                        byteserver::reader::reader(
                            read_fs, read_stream, send).unwrap());

                let write_fs = fs.clone();
                std::thread::spawn(
                    move ||
                        byteserver::writer::writer(
                            write_fs, stream, receive, client).unwrap());
            },
            Err(e) => { println!("WTF {}", e) }
        }
    }
}
