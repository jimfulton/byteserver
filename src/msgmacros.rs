pub use serde::{Deserialize, Serialize};


#[macro_export]
macro_rules! decode {
    ($data: expr, $doing: expr) => (
        {
            let data = $data;
            let mut deserializer = rmp_serde::Deserializer::new(data);
            Deserialize::deserialize(&mut deserializer).context($doing)
        }
    )
}


#[macro_export]
macro_rules! sencode {
    ($data: expr) => (
        {
            let mut buf: Vec<u8> = vec![];
            {
                let mut encoder = rmp_serde::Serializer::new(&mut buf);
                ($data).serialize(&mut encoder).context("encode")
            }.and(Ok(crate::msg::size_vec(buf)))
        }
    )
}


#[macro_export]
macro_rules! message {
    ($id: expr, $method: expr, $data: expr) => (
        sencode!(($id, $method, ($data)))?
    )
}

#[macro_export]
macro_rules! response {
    ($id: expr, $data: expr) => (
        message!($id, "R", ($data))
    )
}

#[macro_export]
macro_rules! error_response {
    ($id: expr, $data: expr) => (
        message!($id, "E", ($data))
    )
}
