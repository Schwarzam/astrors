mod common;

use astrors::io::hdus::primaryhdu::PrimaryHDU;
use std::io::Result;

use polars::frame::DataFrame;
use polars::series::Series;
use polars::prelude::*;


use astrors::io::hdus::table::tablehdu::TableHDU;

#[cfg(test)]
mod tablehdu_tests {
    use std::io::Seek;
    

    use super::*;


    #[test]
    fn tablehdu_test() -> Result<()> {
        use std::fs::File;
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        
        let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
        
        //Seek end_pos 
        f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

        let mut tablehdu = TableHDU::read_from_file(&mut f)?;
        
        println!("Df {:} ", tablehdu.data);
        let outfile = common::get_outtestdata_path("test_table.fits");
        let mut outf = File::create(outfile)?;

        let mut primaryhdu = PrimaryHDU::default();
        primaryhdu.write_to_file(&mut outf)?;
        
        tablehdu.write_to_file(&mut outf)?;

        Ok(())
    }

    #[test]
    fn tablehdu_newtable() -> Result<()> {
        use std::fs::File;
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        
        let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
        
        //Seek end_pos 
        f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

        let tablehdu = TableHDU::read_from_file(&mut f)?;
        
        println!("Df {:} ", tablehdu.data);
        let outfile = common::get_outtestdata_path("test_table.fits");
        let mut outf = File::create(outfile)?;

        let mut primaryhdu = PrimaryHDU::default();
        primaryhdu.write_to_file(&mut outf)?;
        
        let denf = DataFrame::new(vec![
            Series::new("RA", vec![1, 2, 3, 4, 5]),
            Series::new("DEC", vec![1, 2, 3, 4, 5]),
            Series::new("MAG", vec![1, 2, 3, 4, 5]),
        ]).unwrap();
        let mut tablehdu = TableHDU::new_data(denf);
        tablehdu.write_to_file(&mut outf)?;

        Ok(())
    }
}