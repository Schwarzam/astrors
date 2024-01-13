use std::fs::File;
use std::io::{Result, Seek};

use crate::io::Header;
use crate::io::hdus::image::ImageData;

use crate::io::hdus::image::imageops::ImageParser;
use crate::io::header::card::Card;
use crate::io::utils::pad_buffer_to_fits_block;


const MANDATORY_KEYWORDS: [&str; 3] = [
    "SIMPLE",
    "BITPIX",
    "NAXIS",
];

pub struct PrimaryHDU{
    pub header: Header,
    pub data: ImageData,
}

impl PrimaryHDU {
    pub fn new(header: Header, data: ImageData) -> Self {
        Self {
            header,
            data,
        }
    }

    pub fn default() -> Self {
        let mut header = Header::new();
        header.add_card(&Card::new("SIMPLE".to_string(), "T".to_string(), Some("Primary HDU".to_string())));
        header.add_card(&Card::new("BITPIX".to_string(), "8".to_string(), Some("Number of bits per data pixel".to_string())));
        header.add_card(&Card::new("NAXIS".to_string(), "0".to_string(), Some("Number of data axes".to_string())));
        Self {
            header: header,
            data: ImageData::new(),
        }
    }

    pub fn read_from_file(mut f: &mut File) -> Result<Self>  {
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

    pub fn get_end_byte_position(mut f: &mut File) -> usize {
        let first_pos = f.seek(std::io::SeekFrom::Current(0)).unwrap();

        let mut header = Header::new();
        header.read_from_file(&mut f).unwrap();

        if header["NAXIS"].value.as_int().unwrap_or(0) == 0 {
            //actual position after header
            return f.seek(std::io::SeekFrom::Current(0)).unwrap() as usize;
        }

        let current = f.seek(std::io::SeekFrom::Current(0)).unwrap();
        let image_size = ImageParser::calculate_image_bytes(&header);
        let mut end = current + image_size as u64;

        if end % 2880 != 0{
            end = end + 2880 - (end % 2880);
        }
        
        f.seek(std::io::SeekFrom::Start(first_pos)).unwrap();
        end as usize
    }

    pub fn write_to_file(&mut self, mut f: &mut File) -> Result<()> {
        self.header.fix_header_w_mandatory_order(&MANDATORY_KEYWORDS);

        //Check for shape of self.data and write NAXISn keywords
        ImageParser::write_image_header(&mut self.header, &self.data);

        self.header.write_to_buffer(&mut f)?;
        ImageParser::ndarray_to_buffer(&self.data, f)?;
        Ok(())
    }

}