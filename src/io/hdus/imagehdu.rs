use std::fs::File;
use std::io::Result;

use crate::io::Header;
use crate::io::hdus::image::ImageData;

use crate::io::hdus::image::imageops::ImageParser;


const MANDATORY_KEYWORDS: [&str; 3] = [
    "XTENSION",
    "BITPIX",
    "NAXIS",
];

pub struct ImageHDU{
    pub header: Header,
    pub data: ImageData,
}

impl ImageHDU {
    pub fn new(header: Header, data: ImageData) -> Self {
        Self {
            header,
            data,
        }
    }

    pub fn read_from_file(mut f: &mut File) -> Result<Self>  {
        //TODO: Check for mandatory words

        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        
        if !header.are_mandatory_keywords_first(&MANDATORY_KEYWORDS) {
            // TODO: Return a proper error
            // Err(std::io::Error::new(std::io::ErrorKind::Other, "Header corrupted"));
            panic!("Header corrupted");
        }

        let data: ImageData = ImageParser::read_from_buffer(&mut f, &mut header)?;
        Ok(Self::new(header, data))
    }

    pub fn write_to_file(&mut self, mut f: &mut File) -> Result<()> {
        //TODO: This function should not repeat here and in primary hdu
        self.header.fix_header_w_mandatory_order(&MANDATORY_KEYWORDS);

        //Check for shape of self.data and write NAXISn keywords
        ImageParser::write_image_header(&mut self.header, &self.data);

        self.header.write_to_buffer(&mut f)?;
        ImageParser::ndarray_to_buffer(&self.data, f)?;

        Ok(())
    }
}