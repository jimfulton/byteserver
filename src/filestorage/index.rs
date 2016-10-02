// File-storage index-file and mmap index

use std::collections::btree_map::BTreeMap;

use super::util::*;

pub type Index = BTreeMap<Oid, u64>;
    
static MAGIC: &'static [u8] = b"fs2i";

pub fn save_index(index: &Index, path: &str,
              segment_size: u64, start: &Tid, end: &Tid)
              -> io::Result<()> {
    let mut writer = io::BufWriter::new(try!(File::create(path)));
    try!(writer.write_u64::<LittleEndian>(index.len() as u64));
    try!(writer.write_u64::<LittleEndian>(segment_size));
    try!(writer.write_all(start));
    try!(writer.write_all(end));
    for (key, value) in index.iter() {
        try!(writer.write_all(key));
        try!(writer.write_u64::<LittleEndian>(*value));
    }
    Ok(())
}

pub fn load_index(path: &str) -> io::Result<(Index, u64, Tid, Tid)> {
    let mut reader = io::BufReader::new(try!(File::open(path)));
    try!(check_magic(&mut reader, MAGIC));
    let index_length = try!(reader.read_u64::<LittleEndian>());
    let segment_size = try!(reader.read_u64::<LittleEndian>());
    let start = try!(read8(&mut reader));
    let end   = try!(read8(&mut reader));
    let mut index = Index::new();
    for i in 0..index_length {
        index.insert(try!(read8(&mut reader)),
                     try!(reader.read_u64::<LittleEndian>()));
    }
    Ok((index, segment_size, start, end))
}
