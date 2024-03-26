mod common;

use std::{fs::File, io::Read};
use astrors::io::Header;
use astrors::io::hdus::primaryhdu::PrimaryHDU;
use polars::frame::DataFrame;
use std::io::Result;

use polars::series::Series;
use polars::prelude::*;

use astrors::io::hdus::bintable::bintablehdu::BinTableHDU;
use std::{io::{Write, Seek}, ops::Mul, fmt::Error};


#[test]
pub fn bintablehdu_test() -> Result<()> {
    let testfile = common::get_testdata_path("EUVEngc4151imgx.fits");
    let mut f: File = File::open(testfile)?;

    let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
    
    //Seek end_pos 
    f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

    //header.pretty_print_advanced();

    let denf = DataFrame::new(vec![
        Series::new("RA", vec![1, 2, 3, 4, 5]),
        Series::new("DEC", vec![1, 2, 3, 4, 5]),
        Series::new("MAG", vec![1, 2, 3, 4, 5]),
    ]).unwrap();
    let mut bintable = BinTableHDU::new_data(denf);

    println!("Df {:} ", bintable.data);

    let outfile = common::get_outtestdata_path("test_bintable.fits");
    let mut outf = File::create(outfile)?;

    let mut primaryhdu = PrimaryHDU::default();
    primaryhdu.write_to_file(&mut outf)?;
    bintable.write_to_file(&mut outf)?;
    

    Ok(())
}