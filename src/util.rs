use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};

pub fn io_assert(cond: bool, message: &str) -> std::io::Result<()> {
    if cond {
        Ok(())
    } else {
        Err(io_error(message))
    }
}

pub type Tid = [u8; 8];
pub type Oid = [u8; 8];
pub type Bytes = Vec<u8>;

pub static Z64: [u8; 8] = [0u8; 8];
pub fn p64(i: u64) -> [u8; 8] {
    let mut r = [0u8; 8];
    BigEndian::write_u64(&mut r, i);
    r
}

pub fn io_error(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message)
}

pub fn check_magic(
    reader: &mut dyn std::io::Read, magic: &[u8]) -> std::io::Result<()> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    io_assert(&buf == magic, "bad magic")?;
    Ok(())
}

pub fn read_sized(reader: &mut dyn std::io::Read, size: usize)
                  -> std::io::Result<Vec<u8>> {
    if size > 0 {
        let mut r = vec![0u8; size];
        reader.read_exact(&mut r)?;
        Ok(r)
    }
    else {
        Ok(vec![0u8; 0])
    }
}


pub fn read_sized16(reader: &mut dyn std::io::Read) -> std::io::Result<Vec<u8>> {
    let size = reader.read_u16::<BigEndian>()? as usize;
    read_sized(reader, size)
}

pub fn read1(reader: &mut dyn std::io::Read) -> std::io::Result<u8> {
    let mut r = [0u8];
    reader.read_exact(&mut r)?;
    Ok(r[0])
}

pub fn read4(reader: &mut dyn std::io::Read) -> std::io::Result<[u8; 4]> {
    let mut r = [0u8; 4];
    reader.read_exact(&mut r)?;
    Ok::<[u8; 4], std::io::Error>(r)
}

pub fn read8(reader: &mut dyn std::io::Read) -> std::io::Result<[u8; 8]> {
    let mut r = [0u8; 8];
    reader.read_exact(&mut r)?;
    Ok::<[u8; 8], std::io::Error>(r)
}

pub type Ob<T> = std::sync::Arc<std::cell::RefCell<T>>;

pub fn new_ob<T>(v: T) -> Ob<T> {
    std::sync::Arc::new(std::cell::RefCell::new(v))
}

pub fn read_u16(r: &mut dyn std::io::Read) -> std::io::Result<u16> {
    r.read_u16::<BigEndian>()
}

pub fn read_u32(r: &mut dyn std::io::Read) -> std::io::Result<u32> {
    r.read_u32::<BigEndian>()
}

pub fn read_u64(r: &mut dyn std::io::Read) -> std::io::Result<u64> {
    r.read_u64::<BigEndian>()
}

pub fn write_u16(w: &mut dyn std::io::Write, v: u16) -> std::io::Result<()> {
    w.write_u16::<BigEndian>(v)
}

pub fn write_u32(w: &mut dyn std::io::Write, v: u32) -> std::io::Result<()> {
    w.write_u32::<BigEndian>(v)
}

pub fn write_u64(w: &mut dyn std::io::Write, v: u64) -> std::io::Result<()> {
    w.write_u64::<BigEndian>(v)
}

pub fn seek(s: &mut dyn std::io::Seek, pos: u64) -> std::io::Result<u64> {
    s.seek(std::io::SeekFrom::Start(pos))
}


// ======================================================================

pub mod test {

    use tempdir;
    
    pub fn dir() -> tempdir::TempDir {
        tempdir::TempDir::new("test").unwrap()
    }

    pub fn test_path(dir: &tempdir::TempDir, name: &str) -> String {
        String::from(dir.path().join(name).to_str().unwrap())
    }
}
