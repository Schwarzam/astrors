use core::panic;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::fs::File;

use crate::io::header::Header;

use crate::io::hdus::primaryhdu::PrimaryHDU;
use crate::io::hdus::imagehdu::ImageHDU;

use crate::io::hdus::utils::has_more_data;

use super::hdus::primaryhdu;

pub struct HDUList {
    pub hdus: Vec<HDU>,
}


impl HDUList {
    pub fn new() -> Self {
        HDUList {
            hdus: Vec::new(),
        }
    }

    pub fn fromfile(filename : &str) -> Result<Self, std::io::Error> {
        let mut f = File::open(filename)?;
        let mut hdulist = HDUList::new();
        loop {
            let hdu = HDU::read_from_file(&mut f)?;
            hdulist.add_hdu(hdu);
            if !has_more_data(&mut f)? {
                break;
            }
        }
        Ok(hdulist)
    }

    pub fn add_hdu(&mut self, hdu: HDU) {
        self.hdus.push(hdu);
    }

    
}

pub enum HDU {
    Primary(PrimaryHDU),
    Image(ImageHDU),
    //Table(TableHDU),
    //BinTable(BinTableHDU),
}

impl HDU {
    pub fn read_from_file(mut f: &mut File) -> Result<Self, std::io::Error> {
        let current_pos = f.seek(SeekFrom::Current(0))?;

        let mut header = Header::new();
        header.read_from_file(&mut f)?;


        if header.contains_key("SIMPLE") {
            
            f.seek(SeekFrom::Start(current_pos))?;
            let primaryhdu = PrimaryHDU::read_from_file(&mut f)?;
            
            return Ok(HDU::Primary(primaryhdu))

        }else if header.contains_key("XTENSION") {
            let mut hdu_type = header["XTENSION"].value.to_string();
            hdu_type.retain(|c| !c.is_whitespace());
            match hdu_type.as_str() {
                "IMAGE" => {
                    let imagehdu = ImageHDU::read_from_file(&mut f)?;
                    return Ok(HDU::Image(imagehdu))
                },
                "TABLE" => {
                    Err(io::Error::new(io::ErrorKind::Other, "Not implemented TABLE HDU"))
                },
                "BINTABLE" => {
                    Err(io::Error::new(io::ErrorKind::Other, "Not implemented BINTABLE HDU"))
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
