mod utils;
pub mod header;
pub mod hdus;
pub mod hdulist;

pub use self::header::Header;

pub mod compression_algorithms {
    use std::os::raw::c_char;

    extern {
        // ----- ricecomp.c -----
        // - compress
        pub fn fits_rcomp(a: *mut i32, nx: i32, c: *mut u8, clen: i32, nblock: i32) -> i32;
        pub fn fits_rcomp_short(a: *mut i16, nx: i32, c: *mut u8, clen: i32, nblock: i32) -> i32;
        pub fn fits_rcomp_byte(a: *mut i8, nx: i32, c: *mut u8, clen: i32, nblock: i32) -> i32;
        // - decompress
        pub fn fits_rdecomp(c: *mut u8, clen: i32, array: *mut u32, nx: i32, nblock: i32) -> i32;
        pub fn fits_rdecomp_short(c: *mut u8, clen: i32, array: *mut u16, nx: i32, nblock: i32) -> i32;
        pub fn fits_rdecomp_byte(c: *mut u8, clen: i32, array: *mut u8, nx: i32, nblock: i32) -> i32;
        
    
        // ----- quantize.c -----
        pub fn fits_quantize_float(row: i64, fdata: *mut f32, nxpix: i64, nypix: i64, nullcheck: i32, in_null_value: f32, qlevel: f32, dither_method: i32, idata: *mut i32, bscale: *mut f64, bzero: *mut f64, iminval: *mut i32, imaxval: *mut i32) -> i32;
        
        // ----- pliocomp.c -----
        pub fn pl_p2li(pxsrc: *mut i32, xs: i32, lldst: *mut i16, npix: i32) -> i32;
        pub fn pl_l2pi(ll_src: *mut i16, xs: i32, px_dst: *mut i32, npix: i32) -> i32;
        
        // ----- hcompress.c -----
        pub fn fits_hcompress(a: *mut i32, ny: i32, nx: i32, scale: i32, output: *mut c_char, nbytes: *mut i64, status: *mut i32) -> i32;
        pub fn fits_hcompress64(a: *mut i64, ny: i32, nx: i32, scale: i32, output: *mut c_char, nbytes: *mut i64, status: *mut i32) -> i32;
        
        // ----- hdecompress.c -----
        pub fn fits_hdecompress64(input: *mut c_char, smooth: i32, a: *mut i64, ny: *mut i32, nx: *mut i32, scale: *mut i32, status: *mut i32) -> i32;
        pub fn fits_hdecompress(input: *mut c_char, smooth: i32, a: *mut i32, ny: *mut i32, nx: *mut i32, scale: *mut i32, status: *mut i32) -> i32;
        
        //https://github.com/Schwarzam/astrors/blob/2c67c1f76f3cf658f1811d773b87602f05e42573/old/src/main.rs
        // Is a working example of how to use the fits_hcompress and fits_hdecompress functions
    
    
    }
}
