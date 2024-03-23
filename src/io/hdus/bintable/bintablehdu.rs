use std::fs::File;
use std::io::Result;

use polars::frame::DataFrame;
use crate::io::hdus::bintable::bintable::*;

use crate::io::Header;

const MANDATORY_KEYWORDS: [&str; 3] = [
    "XTENSION",
    "BITPIX",
    "NAXIS",
];


pub struct BinTableHDU{
    pub header: Header,
    pub data: DataFrame,
}

impl BinTableHDU {
    pub fn new(header: Header, data: DataFrame) -> Self {
        Self {
            header,
            data,
        }
    }

    pub fn read_from_file(mut f: &mut File) -> Result<Self>  {
        //TODO: Check for mandatory words

        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        
        let mut columns = read_tableinfo_from_header(&header).unwrap();
        let data = fill_columns_w_data(&mut columns, header["NAXIS2"].value.as_int().unwrap_or(0), &mut f);

        let data = columns_to_polars(columns).unwrap();
        // if !header.are_mandatory_keywords_first(&MANDATORY_KEYWORDS) {
        //     // TODO: Return a proper error
        //     // Err(std::io::Error::new(std::io::ErrorKind::Other, "Header corrupted"));
        //     panic!("Header corrupted");
        // }

        // let data: ImageData = ImageParser::read_from_buffer(&mut f, &mut header)?;
        Ok(Self::new(header, data))
    }

    pub fn write_to_file(&mut self, mut f: &mut File) -> Result<()> {
        //TODO: This function should not repeat here and in primary hdu
        self.header.fix_header_w_mandatory_order(&MANDATORY_KEYWORDS);

        let columns = polars_to_columns(self.data.clone()).unwrap();
        create_table_on_header(&mut self.header, &columns);

        self.header.write_to_buffer(&mut f)?;
        columns_to_buffer(columns, &mut f)?;

        Ok(())
    }
}