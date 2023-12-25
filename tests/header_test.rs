extern crate rastronomy;
mod common;

use rastronomy::io::Header;
use rastronomy::io::header::card::Card;
use std::io::Result;

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write};
    use super::*;

    #[test]
    fn read_header() -> Result<()> {
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        //header.pretty_print_advanced();
        Ok(())
    }

    #[test]
    fn read_write_header() -> Result<()> {
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        // header.pretty_print_advanced();
        let output_test = common::get_outtestdata_path("header_outtest.fits");
        let mut outfile: File = File::create(output_test)?;
        header.write_to_buffer(&mut outfile)?;
        outfile.flush()?;
        Ok(())
    }

    #[test]
    fn header_modify_keyword() -> Result<()> {

        // This test should be made after issue https://github.com/Schwarzam/rastronomy/issues/2
        // is resolved. The test should be modified to check if the keyword is actually changed.

        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        // header.pretty_print_advanced();


        let card = &mut header["CTYPE1"];
        card.set_value("TESTANDO".to_string());
        card.set_comment("TESTANDO COMMENT".to_string());

        println!("Len before remove {}", header.len());
        header.remove("CTYPE2");
        println!("Len after remove {}", header.len());
        println!("Header empty {}", header.is_empty());
        header.add_card(Card::new("KEYWORD".to_string(), "1.392122".to_string(), None));
        println!("Float value {}", header["KEYWORD"].value.as_float().unwrap());
        println!("Len after newcard {}", header.len());
        
        let output_test = common::get_outtestdata_path("header_modify_outtest.fits");
        let mut outfile: File = File::create(output_test)?;
        header.write_to_buffer(&mut outfile)?;
        outfile.flush()?;
        Ok(())
        
    }
}