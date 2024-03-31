
use astrors::io::hdus::bintable::bintable::read_tableinfo_from_header;
use astrors::fits;
use astrors::io::hdus::bintable::bintablehdu::BinTableHDU;
use astrors::io::hdus::primaryhdu::PrimaryHDU;

use std::fs::File;
use std::io::{Result, Seek};

mod common;

#[test]
fn test_read_fits() -> Result<()> {

    let file = common::get_testdata_path("0.1_0.1_300_R_swp_splus.fits.fz");

    let mut f: File = File::open(file)?;

    let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
    //Seek end_pos 
    f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

    let bintable = BinTableHDU::read_from_file(&mut f).unwrap();

    println!("{:?}", bintable.data);

    // read_tableinfo_from_header()
    Ok(())
}