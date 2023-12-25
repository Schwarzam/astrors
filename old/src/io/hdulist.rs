use std::io::Result;
use std::fs::File;

use super::image::{ ImageData, ImageParser };
use super::header::Header;
struct HDUList {
    hdus: Vec<HDU>,
}

impl HDUList {
    pub fn new() -> HDUList {
        HDUList {
            hdus: Vec::new(),
        }
    }

    pub fn fromfile(filepath : String) -> Result<HDUList> {
        let hdulist = HDUList::new();
        let mut file: File = File::open(&filepath)?;

        let mut header: Header = crate::io::header::Header::new();
        header.read_from_file(&mut file)?;

        Ok(hdulist)
    }

    pub fn add_hdu(&mut self, hdu: HDU) {
        self.hdus.push(hdu);
    }

    pub fn write_to(&self, mut writer: impl std::io::Write) -> std::io::Result<()> {
        for hdu in &self.hdus {
            hdu.write_to_buffer(&mut writer)?;
        }
        Ok(())
    }
}

enum PrimaryHDU {
    todo()   
}

enum TableData {
    todo()   
}

enum BinTableData {
    todo()
}

enum Data {
    None,
    ImageData(ImageData),
    TableData(TableData),
    BinTableData(BinTableData),
}

// Here all of the data types should be implemented through traits  

impl Data {
    pub fn write_to_buffer(&self, mut writer: impl std::io::Write) -> std::io::Result<()> {
        match self {
            Data::None => {
                todo!()
            },
            Data::ImageData(image_data) => {
                ImageParser::ndarray_to_buffer(image_data, &mut writer);
            },
            Data::TableData(table_data) => {
                todo!()
            },
            Data::BinTableData(bin_table_data) => {
                todo!()
            }
        }
        Ok(())
    }

    pub fn read_from_buffer(&mut self, mut reader: impl std::io::Read) -> std::io::Result<()> {
        match self {
            Data::None => {
                todo!()
            },
            Data::ImageData(image_data) => {
                // ImageParser::buffer_to_ndarray(&mut reader, image_data);
            },
            Data::TableData(table_data) => {
                todo!()
            },
            Data::BinTableData(bin_table_data) => {
                todo!()
            }
        }
        Ok(())
    }
    
}


struct hdu {
    header: Header,
    data: Data,
}

enum HDU {
    PrimaryHDU(hdu),
    ImageHDU(hdu),
    TableHDU(hdu),
    BinTableHDU(hdu),
    CompImage(hdu)
}

impl HDU {
    pub fn write_to_buffer(&self, mut writer: impl std::io::Write) -> std::io::Result<()> {
        match self {
            HDU::PrimaryHDU(hdu) => {
                hdu.header.write_to_buffer(&mut writer)?;
                hdu.data.write_to_buffer(&mut writer)?;
            },
            HDU::ImageHDU(hdu) => {
                hdu.header.write_to_buffer(&mut writer)?;
                hdu.data.write_to_buffer(&mut writer)?;
            },
            HDU::TableHDU(hdu) => {
                hdu.header.write_to_buffer(&mut writer)?;
                hdu.data.write_to_buffer(&mut writer)?;
            },
            HDU::BinTableHDU(hdu) => {
                hdu.header.write_to_buffer(&mut writer)?;
                hdu.data.write_to_buffer(&mut writer)?;
            },
            HDU::CompImage(hdu) => {
                hdu.header.write_to_buffer(&mut writer)?;
                hdu.data.write_to_buffer(&mut writer)?;
            }
        }
        Ok(())
    }
}

#[test]
fn open_fits(){
    use crate::*;

    let hdus = HDUList::fromfile(GLOBAL_FILE_NAME2.clone());


}