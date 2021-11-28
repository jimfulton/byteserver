
pub trait FileFactory {
    fn new(&self) -> std::io::Result<std::fs::File>;
}

#[derive(Debug)]
pub struct ReadFileFactory {
    pub path: String,
}

impl FileFactory for ReadFileFactory {
    fn new(&self) -> std::io::Result<std::fs::File> {
        std::fs::File::open(&self.path)
    }
}

#[derive(Debug)]
pub struct TmpFileFactory {
    base: String,
}

impl TmpFileFactory {
    pub fn base(base: String) -> std::io::Result<TmpFileFactory> {
        {
            if ! std::path::Path::new(&base).exists() {
                std::fs::create_dir(&base)?;
            }
        }
        Ok(TmpFileFactory { base: base })
    }
}

impl FileFactory for TmpFileFactory {
    fn new(&self) -> std::io::Result<std::fs::File> {
        tempfile::tempfile_in(&self.base)
    }
}

pub type TmpFilePointer<'store> = PooledFilePointer<'store, TmpFileFactory>;

#[derive(Debug)]
pub struct FilePool<F: FileFactory> {
    capacity: usize, // Doesn't change
    files: std::sync::Mutex<Vec<std::fs::File>>,
    factory: F, // Doesn't change
}

impl<F: FileFactory> FilePool<F> {
    pub fn new(factory: F, capacity: usize) -> FilePool<F> {
        FilePool { capacity: capacity, factory: factory,
                   files: std::sync::Mutex::new(vec![]) }
    }

    pub fn get<'pool>(&'pool self) -> std::io::Result<PooledFilePointer<'pool, F>> {
        let mut files = self.files.lock().unwrap();
        let file = match files.pop() {
            Some(filerc) => filerc,
            None         => self.factory.new()?,
        };
        Ok(PooledFilePointer {file: file, pool: self})
    }

    pub fn put(&self, filerc: std::fs::File) {
        let mut files = self.files.lock().unwrap();
        if files.len() < self.capacity {
            files.push(filerc);
        }
    }

    pub fn len(&self) -> usize {
        self.files.lock().unwrap().len()
    }
}

unsafe impl<F: FileFactory> std::marker::Sync for FilePool<F> {}
unsafe impl<F: FileFactory> std::marker::Send for FilePool<F> {}

#[derive(Debug)]
pub struct PooledFilePointer<'pool, F: FileFactory + 'pool> {
    file: std::fs::File,
    pool: &'pool FilePool<F>,
}

impl<'pool, F: FileFactory + 'pool> std::ops::Deref for PooledFilePointer<'pool, F> {
    type Target = std::fs::File;

    fn deref<'fptr>(&'fptr self) -> &'fptr std::fs::File {
        &self.file
    }
}

impl<'pool, F: FileFactory + 'pool> Drop for PooledFilePointer<'pool, F> {
    fn drop(&mut self) {
        self.pool.put(self.file.try_clone().expect(r#"Cloning file"#));
    }
}

// ======================================================================

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::prelude::*;
    use std::sync;
    use std::thread;

    use crate::util;

    #[test]
    fn works() {
        let tmp_dir = util::test::dir();
        let sample = b"data";
        let path = String::from(
            tmp_dir.path().join("data").to_str().unwrap());
        { std::fs::File::create(&path).unwrap().write_all(sample).unwrap(); }
        
        let pool = sync::Arc::new(
            FilePool::new(ReadFileFactory { path: path }, 2));
        let (t, r) = sync::mpsc::channel();

        let count = 8;
        
        for i in 0 .. count {
            let tt = t.clone();
            let tpool = pool.clone();
            thread::spawn(move || {
                let p = tpool.get().unwrap();
                let mut file = p.try_clone().unwrap();
                let mut buf = [0u8; 4];
                file.seek(std::io::SeekFrom::Start(0)).unwrap();
                file.read_exact(&mut buf).unwrap();
                tt.send(buf);
            });
        }

        for i in 0 .. count {
            assert_eq!(&r.recv().unwrap(), sample);
        }

    }
}
