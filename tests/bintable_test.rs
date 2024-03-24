mod common;

use std::{fs::File, io::Read};
use astrors::io::Header;
use astrors::io::hdus::primaryhdu::PrimaryHDU;
use std::io::Result;

use astrors::io::hdus::bintable::bintable::*;

use astrors::io::hdus::bintable::bintablehdu::BinTableHDU;


#[cfg(test)]
mod tablehdu_tests {
    use std::{fs::File, io::{Write, Seek}, ops::Mul, fmt::Error};

    use super::*;

    #[test]
    fn read_bintablehdu() -> Result<()> {
        
        let testfile = common::get_testdata_path("EUVEngc4151imgx.fits");
        let mut f: File = File::open(testfile)?;
    
        let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
        
        //Seek end_pos 
        f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

        let mut buffer = vec![0; 2880];

        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        
        let mut columns = read_tableinfo_from_header(&header).unwrap();
        println!("Columns: {:?}", columns);
        
        let df = read_table_bytes_to_df(&mut columns, header["NAXIS2"].value.as_int().unwrap_or(0), &mut f);
        
        //println!("DF: {:?}", df);
        //let columns = polars_to_columns(df.unwrap()).unwrap();
        
        
        //let outfile = common::get_outtestdata_path("test_bintable.fits");
        //let mut outf = File::create(outfile)?;
        
        //create_table_on_header(&mut header, &columns);

        //let mut primaryhdu = PrimaryHDU::default();
        //primaryhdu.write_to_file(&mut outf)?;

        //header.write_to_buffer(&mut outf)?;
        //columns_to_buffer(columns, &mut outf)?;


        Ok(())
    }

    #[test]
    pub fn bintablehdu_test() -> Result<()> {
        let testfile = common::get_testdata_path("EUVEngc4151imgx.fits");
        let mut f: File = File::open(testfile)?;
    
        let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
        


        //Seek end_pos 
        f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

        let mut header = Header::new();
        header.read_from_file(&mut f)?;

        header.pretty_print_advanced();
        // let mut bintable = BinTableHDU::read_from_file(&mut f)?;
        
        // //println!("Df {:} ", bintable.data);

        // let outfile = common::get_outtestdata_path("test_bintable.fits");
        // let mut outf = File::create(outfile)?;

        // let mut primaryhdu = PrimaryHDU::default();
        //primaryhdu.write_to_file(&mut outf)?;
        
        //bintable.write_to_file(&mut outf)?;

        Ok(())
    }

}

