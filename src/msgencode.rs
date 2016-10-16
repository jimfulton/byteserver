// Helpers to encode sized msgpack records

use byteorder::{ByteOrder, BigEndian};
pub use rmp_serde;
pub use serde::Serialize;

pub fn size_vec(mut v: Vec<u8>) -> Vec<u8> {
    let l = v.len();
    for i in 0..4 {
        v.insert(0, 0);
    }
    BigEndian::write_u32(&mut v, l as u32);
    v
}

#[macro_export]
macro_rules! sencode {
    ($data: expr) => (
        {
            let mut buf: Vec<u8> = vec![];
            {
                let mut encoder = rmp_serde::Serializer::new(&mut buf);
                ($data).serialize(&mut encoder).chain_err(|| "encode")
            }.and(Ok(size_vec(buf)))
        }
    )
}


// ======================================================================

#[cfg(test)]
mod tests {

    use super::*;
    use errors::*;

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
