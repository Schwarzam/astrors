mod common;

use astrors::io::hdus::primaryhdu::PrimaryHDU;
use astrors::io::hdulist::HDUList;
use astrors::io::get_data;
use astrors::fits;

use polars::series::Series;
use polars::prelude::*;

use astrors::io::hdus::bintable::bintablehdu::BinTableHDU;

use std::io::Result;

#[cfg(test)]
mod hdu_tests {
    use std::fs::File;
    use astrors::io::{hdulist::HDU, hdus::image::ImageData};

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
    fn getdata() -> Result<()> {
        let testfile = common::get_testdata_path("WFPC2u57_2.fits");
        
        let data = get_data(testfile.to_str().unwrap());

        println!("Data: {:?}", data);

        Ok(())
    }

    #[test]
    fn read_other_fits() -> Result<()> {
        let testfile = common::get_testdata_path("EUVEngc4151imgx.fits");

        let mut hdu_list = fits::fromfile(testfile.to_str().unwrap())?;

        for hdu in hdu_list.hdus.iter() {

            if let HDU::Primary(primary_hdu) = hdu {
                println!("Primary HDU: {:?}", primary_hdu.data);
            }

            if let HDU::BinTable(image_hdu) = hdu {
                println!("Bin table HDU: {:?}", image_hdu.data);
            }

            println!("HDU: {:?}", hdu)
        }

        let outfile = common::get_outtestdata_path("EUVEngc4151imgx_written_by_astrors.fits");
        hdu_list.write_to(outfile.to_str().unwrap())?;

        println!("HDU List: {:?}", hdu_list.hdus.len());

        Ok(())
    }

    #[test]
    fn read_primary_hdu() -> Result<()> {
        
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        
        let mut primary_hdu = PrimaryHDU::read_from_file(&mut f)?;
        
        let outfile = common::get_outtestdata_path("primary_hdu_test.fits");
        let mut f_out: File = File::create(outfile)?;
        primary_hdu.write_to_file(&mut f_out)?;
        //println!("Primary HDU: {:?}", primary_hdu.data);
        
        //println!("Has more data: {}", buffer_has_more_data(&mut f)?);
        //header.pretty_print_advanced();
        
        Ok(())
    }

    #[test]
    fn read_primary_hdu_modify() -> Result<()> {
        
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        
        let mut primary_hdu = PrimaryHDU::read_from_file(&mut f)?;
        
        let outfile = common::get_outtestdata_path("test_modify.fits");
        let f_out: File = File::create(&outfile)?;

        let mut hdus = HDUList::new();

        use ndarray::{ArrayD, IxDyn};
        primary_hdu.data = 
            ImageData::F32(
                ArrayD::from_elem(IxDyn(&[100, 100]), 1.0)
            );

        hdus.add_hdu(HDU::Primary(primary_hdu));
        //println!("Primary HDU: {:?}", primary_hdu.data);
        
        let df = DataFrame::new(vec![
            Series::new("RA", vec![1, 2, 3, 4, 5]),
            Series::new("DEC", vec![1, 2, 3, 4, 5]),
            Series::new("MAG", vec![1, 2, 3, 4, 5]),
        ]).unwrap();
        
        let bintable = BinTableHDU::new_data(df);
        hdus.add_hdu(HDU::BinTable(bintable));

        hdus.write_to(outfile.to_str().unwrap())?;

        Ok(())
    }

    #[test]
    fn test() -> Result<()>{
        // let mut hdu_list = fits::fromfile("/Users/gustavo/Downloads/SPLUS_DR4_stparam_SPHINX_v1.fits");
        
        //hdu_list?.write_to("C:/Users/gusta/Downloads/teste.fits")?;
        Ok(())
    }

}

