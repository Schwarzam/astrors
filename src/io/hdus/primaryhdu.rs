use std::fs::File;
use std::io::{Result, Seek};

use crate::io::Header;
use crate::io::hdus::image::ImageData;

use crate::io::hdus::image::image::ImageParser;
use crate::io::header::card::Card;


const MANDATORY_KEYWORDS: [&str; 3] = [
    "SIMPLE",
    "BITPIX",
    "NAXIS",
];

/// Represents the Primary HDU (Header Data Unit) of a FITS file.
///
/// This struct encapsulates the header and image data of the Primary HDU.
/// It provides functionality for creating, reading, and writing the Primary HDU.
pub struct PrimaryHDU{
    pub header: Header,
    pub data: ImageData,
}

impl PrimaryHDU {
    /// Creates a new `PrimaryHDU` instance with the provided header and image data.
    ///
    /// # Arguments
    /// - `header` (Header): The header associated with the HDU.
    /// - `data` (ImageData): The image data for the HDU.
    ///
    /// # Returns
    /// - `PrimaryHDU`: A new instance of the Primary HDU.
    pub fn new(header: Header, data: ImageData) -> Self {
        Self {
            header,
            data,
        }
    }

    /// Creates a default `PrimaryHDU` with minimal header information.
    ///
    /// # Returns
    /// - `PrimaryHDU`: A default Primary HDU with "SIMPLE", "BITPIX", and "NAXIS" header keywords.
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

    /// Reads a `PrimaryHDU` from a file.
    ///
    /// # Arguments
    /// - `f` (&mut File): The file handle to read from.
    ///
    /// # Returns
    /// - `Result<PrimaryHDU>`: The Primary HDU read from the file.
    ///
    /// # Behavior
    /// - Checks if the mandatory keywords are present and in order.
    /// - Returns an empty `ImageData` if `NAXIS` is 0.
    pub fn read_from_file(mut f: &mut File) -> Result<Self>  {
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        
        if !header.are_mandatory_keywords_first(&MANDATORY_KEYWORDS) {
            // TODO: Return a proper error
            // Err(std::io::Error::new(std::io::ErrorKind::Other, "Header corrupted"));
            panic!("Header corrupted");
        }

        if header["NAXIS"].value.as_int().unwrap_or(0) == 0 {
            //actual position after header
            return Ok(Self::new(header, ImageData::EMPTY));
        }
        else {
            let data: ImageData = ImageParser::read_from_buffer(&mut f, &mut header)?;
            Ok(Self::new(header, data))
        }
    }

    /// Calculates the byte position of the end of the Primary HDU in the file.
    ///
    /// # Arguments
    /// - `f` (&mut File): The file handle.
    ///
    /// # Returns
    /// - `usize`: The byte position of the end of the Primary HDU.
    ///
    /// # Behavior
    /// - Calculates the size of the image data and adjusts to the next 2880-byte boundary.
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

    /// Writes the `PrimaryHDU` to a file.
    ///
    /// # Arguments
    /// - `f` (&mut File): The file handle to write to.
    ///
    /// # Returns
    /// - `Result<()>`: Indicates whether the operation was successful.
    ///
    /// # Behavior
    /// - Ensures the mandatory keywords are ordered correctly.
    /// - Writes the header and image data, if present.
    pub fn write_to_file(&mut self, mut f: &mut File) -> Result<()> {
        self.header.fix_header_w_mandatory_order(&MANDATORY_KEYWORDS);
        
        //Check for shape of self.data and write NAXISn keywords
        ImageParser::write_image_header(&mut self.header, &self.data);

        self.header.write_to_buffer(&mut f)?;

        if self.data.get_shape()[0] == 0 {
            return Ok(());
        }
        ImageParser::ndarray_to_buffer(&self.data, f)?;
        Ok(())
    }

}