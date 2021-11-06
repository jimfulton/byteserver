// File-storage index-file and mmap index

use std::collections::btree_map::BTreeMap;

use crate::util::*;

pub type Index = BTreeMap<Oid, u64>;
    
static MAGIC: &'static [u8] = b"fs2i";

pub fn save_index(index: &Index, path: &str,
              segment_size: u64, start: &Tid, end: &Tid)
              -> io::Result<()> {
    let mut writer = io::BufWriter::new(File::create(path)?);
    writer.write_all(MAGIC)?;
    writer.write_u64::<BigEndian>(index.len() as u64)?;
    writer.write_u64::<BigEndian>(segment_size)?;
    writer.write_all(start)?;
    writer.write_all(end)?;
    for (key, value) in index.iter() {
        writer.write_all(key)?;
        writer.write_u64::<BigEndian>(*value)?;
    }
    Ok(())
}

pub fn load_index(path: &str) -> io::Result<(Index, u64, Tid, Tid)> {
    let mut reader = io::BufReader::new(File::open(path)?);
    check_magic(&mut reader, MAGIC)?;
    let index_length = reader.read_u64::<BigEndian>()?;
    let segment_size = reader.read_u64::<BigEndian>()?;
    let start = read8(&mut reader)?;
    let end   = read8(&mut reader)?;
    let mut index = Index::new();
    for i in 0..index_length {
        index.insert(read8(&mut reader)?,
                     reader.read_u64::<BigEndian>()?);
    }
    Ok((index, segment_size, start, end))
}

// ======================================================================

#[cfg(test)]
mod tests {

    use super::*;
    use crate::util;

    #[test]
    fn works() {
        let mut index = Index::new();

        for i in 0..10 {
            index.insert(p64(i), i*999);
        }

        let tmpdir = util::test::dir();

        let path = String::from(tmpdir.path().join("index").to_str().unwrap());
        let segment_size = 9999u64;
        let start = p64(1);
        let end = p64(1234567890);
        
        save_index(&index, &path, segment_size, &start, &end).unwrap();

        assert_eq!(load_index(&path).unwrap(),
                   (index, segment_size, start, end));
    }
}
