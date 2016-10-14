#![feature(plugin)]
#![recursion_limit = "1024"]
#![allow(dead_code, unused_must_use, unused_variables)]

extern crate byteorder;
extern crate rmp;
extern crate rmp_serde;
extern crate serde;
extern crate tempdir;
extern crate tempfile;
extern crate time;

#[macro_use]
extern crate error_chain;

#[macro_use]
mod util;

mod errors;
mod storage;
mod index;
mod lock;
mod msgparse;
mod pool;
mod records;
mod server;
mod tid;
mod transaction;
