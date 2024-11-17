use core::panic;
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Seek, SeekFrom};
use std::fs::File;

use crate::io::header::Header;

use crate::io::hdus::primaryhdu::PrimaryHDU;
use crate::io::hdus::image::imagehdu::ImageHDU;
use crate::io::hdus::bintable::bintablehdu::BinTableHDU;
use crate::io::hdus::table::tablehdu::TableHDU;

use crate::io::hdus::utils::buffer_has_more_data;


pub struct HDUList {
    pub hdus: Vec<HDU>,
}

impl Debug for HDUList{

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.hdus.iter().for_each(|hdu| {
            write!(f, "{:?}", hdu).unwrap();
        });
        Ok(())
    }
    
}


/// Represents a list of Header Data Units (HDUs).
/// 
/// An HDUList is a collection of HDUs, where each HDU contains data and metadata.
/// It provides methods for creating a new HDUList, reading from a file, and adding HDUs to the list.
impl Default for HDUList {
    fn default() -> Self {
        Self::new()
    }
}

impl HDUList {
    /// Creates a new empty HDUList.
    pub fn new() -> Self {
        HDUList {
            hdus: Vec::new(),
        }
    }

    /// Reads an HDUList from a file.
    /// 
    /// # Arguments
    /// 
    /// * `filename` - The path to the file to read from.
    /// 
    /// # Returns
    /// 
    /// Returns a Result containing the HDUList if successful, or an std::io::Error if an error occurred.
    pub fn fromfile(filename : &str) -> Result<Self, std::io::Error> {
        let mut f = File::open(filename)?;
        let mut hdulist = HDUList::new();
        let mut primary_hdu = true;
        loop {
            
            let hdu = HDU::read_from_file(&mut f, Some(primary_hdu));
            //TODO: implement own notimplemented error
            let hdu = match hdu {
                Ok(hdu) => hdu,
                Err(e) => {
                    println!("Error reading HDU: {}", e);
                    continue;
                }
            };
            primary_hdu = false;
            hdulist.add_hdu(hdu);

            if !buffer_has_more_data(&mut f)? {
                break;
            }
        }
        Ok(hdulist)
    }

    pub fn write_to(&mut self, filename: &str) -> Result<(), std::io::Error> {
        let mut f = File::create(filename)?;
        for hdu in &mut self.hdus {
            match hdu {
                HDU::Primary(hdu)   => hdu.write_to_file(&mut f)?,
                HDU::Image(hdu)       => hdu.write_to_file(&mut f)?,
                HDU::Table(hdu)       => hdu.write_to_file(&mut f)?,
                HDU::BinTable(hdu) => hdu.write_to_file(&mut f)?,
            }
        }
        Ok(())
    }

    /// Adds an HDU to the HDUList.
    /// 
    /// # Arguments
    /// 
    /// * `hdu` - The HDU to add to the list.
    pub fn add_hdu(&mut self, hdu: HDU) {
        self.hdus.push(hdu);
    }
}

pub enum HDU {
    Primary(PrimaryHDU),
    Image(ImageHDU),
    Table(TableHDU),
    BinTable(BinTableHDU),
}

impl fmt::Debug for HDU {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HDU::Primary(hdu) => write!(f, "<Primary HDU object at memory location {:p}>", hdu),
            HDU::Image(hdu) => write!(f, "<Image HDU object at memory location {:p}>", hdu),
            HDU::Table(hdu) => write!(f, "<Table HDU object at memory location {:p}>", hdu),
            HDU::BinTable(hdu) => write!(f, "<BinTable HDU object at memory location {:p}>", hdu),
        }
    }
}

/// Represents an HDU (Header Data Unit) in a FITS (Flexible Image Transport System) file.
impl HDU {
    /// Reads an HDU from a file.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable reference to a `File` object representing the FITS file.
    /// * `primary_hdu` - An optional boolean value indicating whether the HDU is the primary HDU.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the parsed HDU if successful, or an `std::io::Error` if an I/O error occurs.
    pub fn read_from_file(f: &mut File, primary_hdu: Option<bool>) -> Result<Self, std::io::Error> {
        let current_pos = f.stream_position()?;
        let mut header = Header::new();
        header.read_from_file(f)?;
        
        if primary_hdu.unwrap_or(false) {
            f.seek(SeekFrom::Start(current_pos))?;
            let primaryhdu = PrimaryHDU::read_from_file(f)?;
            Ok(HDU::Primary(primaryhdu))

        } else if header.contains_key("XTENSION") {
            let mut hdu_type = header["XTENSION"].value.to_string();
            hdu_type.retain(|c| !c.is_whitespace());
            match hdu_type.as_str() {
                "IMAGE" => {
                    f.seek(SeekFrom::Start(current_pos))?;
                    let imagehdu = ImageHDU::read_from_file(f)?;
                    return Ok(HDU::Image(imagehdu))
                },
                "TABLE" => {
                    f.seek(SeekFrom::Start(current_pos))?;
                    let tablehdu = TableHDU::read_from_file(f)?;
                    return Ok(HDU::Table(tablehdu))
                },
                "BINTABLE" => {
                    f.seek(SeekFrom::Start(current_pos))?;
                    let bintablehdu = BinTableHDU::read_from_file(f)?;
                    return Ok(HDU::BinTable(bintablehdu))
                },
                _ => {
                    Err(io::Error::new(io::ErrorKind::Other, "Not implemented HDU type"))
                }
                
            }
        } else {
            panic!("Not implemented");
        }
    }
}
