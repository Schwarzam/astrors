mod utils;
pub mod header;
pub mod hdus;
pub mod hdulist;

pub use self::header::Header;

use std::os::raw::c_char;
extern {

    //https://github.com/Schwarzam/astrors/blob/2c67c1f76f3cf658f1811d773b87602f05e42573/old/src/main.rs
    // Is a working example of how to use the fits_hcompress and fits_hdecompress functions

    pub fn fits_hcompress(a: *mut i32, ny: i32, nx: i32, scale: i32, output: *mut c_char, nbytes: *mut i64, status: *mut i32) -> i32;
    pub fn fits_hdecompress(input: *mut c_char, output: *mut i32, ny: i32, nx: i32, status: *mut i32) -> i32;

    //TODO: Add signatures for all the other C functions
}