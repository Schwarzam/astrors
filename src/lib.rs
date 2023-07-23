#![allow(non_camel_case_types)]
#![allow(unused)]
use lazy_static::lazy_static;

// Declare the global variable with the desired type
lazy_static! {
    static ref GLOBAL_FILE_NAME: String = {
        // Put the file name here
        // "/Users/gustavo/Downloads/bpmask_proc_SPLUS-GAL-20180325-043054.fits".to_string()
        "/Users/gustavo/Downloads/test.fits".to_string()

    };
}

mod io {
    pub mod header;
    pub mod image;
    pub mod aux;
}


