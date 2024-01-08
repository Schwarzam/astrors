mod common;

use astrors::io::Header;
use astrors::io::hdus::primaryhdu::PrimaryHDU;
use std::io::Result;

use astrors::io::hdus::table::table::read_tableinfo_from_header;

#[cfg(test)]
mod tablehdu_tests {
    use std::{fs::File, io::{Write, Seek}, ops::Mul, fmt::Error};
    use astrors::io::hdus::table::table::fill_columns_w_data;

    use super::*;

    #[test]
    fn read_tablehdu() -> Result<()> {
        use std::{fs::File, io::Read};
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        
        //header.pretty_print_advanced();

        let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
        //Seek end_pos 
        f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

        let mut buffer = vec![0; 2880];

        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        
        let mut columns = read_tableinfo_from_header(&header).unwrap();
        //println!("Columns: {:?}", res);
        let data = fill_columns_w_data(&mut columns, header["NAXIS2"].value.as_int().unwrap_or(0), &mut f);
        
        for column in &columns{
            println!("{:?}", column)
        }

        Ok(())
    }

}