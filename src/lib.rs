#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]
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

mod filestorage;
mod msgparse;
mod server;
