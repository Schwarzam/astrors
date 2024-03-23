mod common;

use astrors::io::Header;
use astrors::io::hdus::primaryhdu::PrimaryHDU;
use std::io::Result;

use astrors::io::hdus::table::table::*;

use astrors::io::hdus::table::tablehdu::TableHDU;

#[cfg(test)]
mod tablehdu_tests {
    use std::{fs::File, io::{Write, Seek}, ops::Mul, fmt::Error};
    use astrors::io::hdus::{table::table::polars_to_columns, primaryhdu};

    use super::*;

    #[test]
    fn read_tablehdu() -> Result<()> {
        use std::{fs::File, io::Read};
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
    
        let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
        
        //Seek end_pos 
        f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

        let mut buffer = vec![0; 2880];

        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        
        let mut columns = read_tableinfo_from_header(&header).unwrap();
        //println!("Columns: {:?}", res);
        let data = fill_columns_w_data(&mut columns, header["NAXIS2"].value.as_int().unwrap_or(0), &mut f);
        
        let mut df = columns_to_polars(columns).unwrap();

        let last_row = df.tail(Some(1));
        // Append the cloned row to the DataFrame
        df.vstack_mut(&last_row);

        // println!("DF: {:?}", df);

        let columns = polars_to_columns(df).unwrap();
        
        let outfile = common::get_outtestdata_path("test_table.fits");
        let mut outf = File::create(outfile)?;
        
        create_table_on_header(&mut header, &columns);

        let mut primaryhdu = PrimaryHDU::default();
        primaryhdu.write_to_file(&mut outf)?;

        header.write_to_buffer(&mut outf)?;
        columns_to_buffer(columns, &mut outf)?;

        Ok(())
    }

    #[test]
    fn tablehdu_test() -> Result<()> {
        use std::{fs::File, io::Read};
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
}