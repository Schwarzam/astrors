mod common;

use astrors::io::hdus::primaryhdu::PrimaryHDU;
use astrors::io::hdus::utils::buffer_has_more_data;
use astrors::io::hdulist::HDUList;
use astrors::fits;


use std::io::Result;

#[cfg(test)]
mod hdu_tests {
    use std::{fs::File, io::{Write, Seek}};
    use astrors::io::{Header, hdus::image::ImageData};

    use super::*;

    #[test]
    fn read_fits() -> Result<()> {
        let testfile = common::get_testdata_path("WFPC2u57_2.fits");
        
        let mut hdu_list = fits::fromfile(testfile.to_str().unwrap())?;

        let outfile = common::get_outtestdata_path("WFPC2u57_2_written_by_astrors.fits");
        hdu_list.write_to(outfile.to_str().unwrap())?;

        println!("HDU List: {:?}", hdu_list.hdus.len());

        Ok(())
    }

    #[test]
    // fn read_other_fits() -> Result<()> {
    //     let testfile = common::get_testdata_path("EUVEngc4151imgx.fits");
        
    //     let mut hdu_list = fits::fromfile(testfile.to_str().unwrap())?;

    //     // let outfile = common::get_outtestdata_path("EUVEngc4151imgx_written_by_astrors.fits");
    //     // hdu_list.write_to(outfile.to_str().unwrap())?;

    //     println!("HDU List: {:?}", hdu_list.hdus.len());

    //     Ok(())
    // }

    #[test]
    fn read_primary_hdu() -> Result<()> {
        
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        
        let mut primary_hdu = PrimaryHDU::read_from_file(&mut f)?;
        
        let outfile = common::get_outtestdata_path("primary_hdu_test.fits");
        let mut f_out: File = File::create(outfile)?;
        primary_hdu.write_to_file(&mut f_out)?;
        //println!("Primary HDU: {:?}", primary_hdu.data);
        
        println!("Has more data: {}", buffer_has_more_data(&mut f)?);
        //header.pretty_print_advanced();
        
        Ok(())
    }

    #[test]
    fn read_primary_hdu_modify() -> Result<()> {
        
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        
        let mut primary_hdu = PrimaryHDU::read_from_file(&mut f)?;
        
        let outfile = common::get_outtestdata_path("primary_hdu_test_modify.fits");
        let mut f_out: File = File::create(outfile)?;

        use ndarray::{ArrayD, IxDyn};
        primary_hdu.data = 
            ImageData::F32(
                ArrayD::from_elem(IxDyn(&[100, 100]), 1.0)
            );

        primary_hdu.write_to_file(&mut f_out)?;
        //println!("Primary HDU: {:?}", primary_hdu.data);
        
        println!("Has more data: {}", buffer_has_more_data(&mut f)?);
        //header.pretty_print_advanced();
        
        //Reading the next header if more data is available
        let mut header = Header::new();
        

        Ok(())
    }

    #[test]
    fn test(){

    }

}

