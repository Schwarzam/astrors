extern crate rastronomy;
mod common;

use rastronomy::io::header::Header;
use std::io::Result;

#[cfg(test)]
mod tests {
    use std::{fs::File, io::{Error, Write}};
    use super::*;

    #[test]
    fn read_header() -> Result<()> {
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        // header.pretty_print_advanced();
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
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        // header.pretty_print_advanced();
        header.modify_keyword("SIMPLE", "T");
        header.get_card("CTYPE1").unwrap().set_value("TESTING");
        


        let output_test = common::get_outtestdata_path("header_modify_outtest.fits");
        let mut outfile: File = File::create(output_test)?;
        header.write_to_buffer(&mut outfile)?;
        outfile.flush()?;
        Ok(())
}