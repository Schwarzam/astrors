

mod io {
    pub mod header;
    pub mod image;
    pub mod aux;
}

#[test]
fn read_test() -> std::io::Result<()>{
    // crate::fits_io::read_file();

    rayon::ThreadPoolBuilder::new().num_threads(8).build_global().unwrap();

    use std::fs::File;
    // let mut f = File::open("./testdata/test.fits")?
    let mut f: File = File::open("/Users/gustavo/Downloads/bpmask_proc_SPLUS-GAL-20180325-043054.fits")?;

    let mut header = crate::io::header::Header::new();
    header.read_from_filebytes(&mut f)?;

    header.pretty_print();
    
    crate::io::image::Data::read_from_filebytes(&mut f, &mut header)?;

    use rayon::prelude::*;
    println!("{} threads", rayon::current_num_threads());

    Ok(())
}
