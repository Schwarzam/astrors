#![allow(non_camel_case_types)]
#![allow(unused)]
use lazy_static::lazy_static;

// Declare the global variable with the desired type
lazy_static! {
    static ref GLOBAL_FILE_NAME: String = {
        // Put the file name here
        // "/Users/gustavo/Downloads/bpmask_proc_SPLUS-GAL-20180325-043054.fits".to_string()
        "/Users/gustavo/Downloads/j02-FRIN-b20201225e0125-Z-00-MainSurvey.fits".to_string()
        // "/Users/gustavo/Downloads/test.fits".to_string()

    };

    static ref GLOBAL_FILE_NAME2: String = {
        // Put the file name here
        // "/Users/gustavo/Downloads/bpmask_proc_SPLUS-GAL-20180325-043054.fits".to_string()
        "/Users/gustavo/rust-astro/testdata/WFPC2u5780205r_c0fx.fits".to_string()
        // "/Users/gustavo/Downloads/test.fits".to_string()

    };

    static ref WRITE_FILE: String = {
        "/Users/gustavo/Downloads/output2.fits".to_string()
    };
}

mod io {
    pub mod header;
    pub mod image;
    pub mod utils;
    pub mod hdulist;
}


