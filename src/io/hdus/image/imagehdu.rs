use std::fs::File;
use std::io::Result;

use crate::io::Header;
use crate::io::hdus::image::ImageData;

use crate::io::hdus::image::image::ImageParser;


const MANDATORY_KEYWORDS: [&str; 3] = [
    "XTENSION",
    "BITPIX",
    "NAXIS",
];

pub struct ImageHDU{
    pub header: Header,
    pub data: ImageData,
}

/// Represents an Image Header Data Unit (HDU) in a FITS file.
///
/// This struct encapsulates the header and image data of a FITS file.
/// It provides methods to read from and write to FITS files while ensuring
/// compliance with the FITS standard.
impl ImageHDU {
    /// Creates a new `ImageHDU` instance with the provided header and data.
    ///
    /// # Parameters:
    /// - `header`: The header containing metadata for the image.
    /// - `data`: The image data associated with the header.
    ///
    /// # Returns:
    /// - A new `ImageHDU` instance.
    pub fn new(header: Header, data: ImageData) -> Self {
        Self {
            header,
            data,
        }
    }

    /// Reads an Image HDU from a file.
    ///
    /// This function reads the header and data sections of a FITS file and creates
    /// an `ImageHDU` instance. It ensures that the mandatory keywords appear in
    /// the correct order at the start of the header.
    ///
    /// # Parameters:
    /// - `f`: A mutable reference to a file object.
    ///
    /// # Returns:
    /// - `Ok(Self)`: An `ImageHDU` instance containing the header and data.
    /// - `Err`: If reading from the file fails or the header is corrupted.
    pub fn read_from_file(f: &mut File) -> Result<Self>  {
        //TODO: Check for mandatory words

        let mut header = Header::new();
        header.read_from_file(f)?;
        
        if !header.are_mandatory_keywords_first(&MANDATORY_KEYWORDS) {
            // TODO: Return a proper error
            // Err(std::io::Error::new(std::io::ErrorKind::Other, "Header corrupted"));
            panic!("Header corrupted");
        }

        let data: ImageData = ImageParser::read_from_buffer(f, &mut header)?;
        Ok(Self::new(header, data))
    }

    /// Writes the Image HDU to a file.
    ///
    /// This function writes the header and image data to a file in the FITS format.
    /// It ensures that the mandatory keywords are in the correct order and updates
    /// the header with the correct `NAXISn` keywords based on the image shape.
    ///
    /// # Parameters:
    /// - `f`: A mutable reference to a file object.
    ///
    /// # Returns:
    /// - `Ok(())`: If the HDU is successfully written to the file.
    /// - `Err`: If writing to the file fails.
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