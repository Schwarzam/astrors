mod common;

use rastronomy::io::hdus::primaryhdu::PrimaryHDU;
use rastronomy::io::hdus::utils::has_more_data;
use rastronomy::io::hdulist::HDUList;

use std::io::Result;

#[cfg(test)]
mod image_tests {
    use std::{fs::File, io::{Write, Seek}};
    use rastronomy::io::Header;

    use super::*;

    #[test]
    fn read_fits() -> Result<()> {
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        
        let hdu = HDUList::fromfile(testfile.to_str().unwrap());
        

        Ok(())
    }

    #[test]
    fn read_primary_hdu() -> Result<()> {
        
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        
        let primary_hdu = PrimaryHDU::read_from_file(&mut f)?;
        
        let outfile = common::get_outtestdata_path("primary_hdu_test.fits");
        let mut f_out: File = File::create(outfile)?;
        primary_hdu.write_to_file(&mut f_out)?;
        //println!("Primary HDU: {:?}", primary_hdu.data);
        
        println!("Has more data: {}", has_more_data(&mut f)?);
        //header.pretty_print_advanced();
        
        //Reading the next header if more data is available
        let mut header = Header::new();
        

        Ok(())
    }
}