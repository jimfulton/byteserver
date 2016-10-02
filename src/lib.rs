#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]
#![recursion_limit = "1024"]
#![allow(dead_code, unused_must_use, unused_variables)]

extern crate byteorder;
extern crate tempdir;
extern crate tempfile;

#[macro_use]
extern crate error_chain;

mod filestorage;
