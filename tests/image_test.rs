mod common;

use astrors::io::Header;
use astrors::io::hdus::image::image::{ImageParser, ImageData};
use std::io::Result;

#[cfg(test)]
mod image_tests {
    use std::fs::File;
    use super::*;

    #[test]
    fn read_image() -> Result<()> {
        
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        //header.pretty_print_advanced();

        let data: ImageData = ImageParser::read_from_buffer(&mut f, &mut header)?;
        if let ImageData::F32(array) = data {
            println!("Mean {:?}", array.mean().unwrap());
        } else {
            panic!("Not implemented for test");
        }

        Ok(())
    }

    #[test]
    fn modify_and_write_image() -> Result<()> {
        
        let testfile = common::get_testdata_path("WFPC2u57.fits");
        let mut f: File = File::open(testfile)?;
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        //header.pretty_print_advanced();

        let mut data = ImageParser::read_from_buffer(&mut f, &mut header)?;
        // println!("Data: {:?}", data);
        
        if let ImageData::F32(ref array) = data {
            println!("Mean before mult {:?}", array.mean().unwrap());
        }

        if let ImageData::F32(ref mut array) = data {
            // Multiply all elements in the array by 2.0
            *array *= 2.0;
        
            // Print the shape of the array
            let shape = array.shape();
            println!("Shape of the modified array: {:?}", shape);
        
            // Use shape to update NAXIS values or for other purposes
            // For example: 
            // let naxis1 = shape[0];
            // let naxis2 = shape.get(1).cloned().unwrap_or(0); // This is just an example
            // ...
        } else {
            panic!("Not implemented for test");
        }

        if let ImageData::F32(ref array) = data {
            println!("Mean after mult {:?}", array.mean().unwrap());
        }

        // println!("Data: {:?}", data);

        let outpath = common::get_outtestdata_path("imagemodified.fits");
        let mut f = File::create(outpath)?;
        header.write_to_buffer(&mut f)?;

        ImageParser::write_to_buffer(&data, &mut f)?;

        Ok(())
    }

}