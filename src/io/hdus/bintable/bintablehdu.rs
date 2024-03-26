use std::fs::File;
use std::io::Result;

use polars::frame::DataFrame;
use crate::io::hdus::bintable::bintable::*;

use crate::io::header::card::Card;
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

    pub fn new_data(data: DataFrame) -> Self {
        let mut header = Header::new();
        header.add_card(&Card::new("XTENSION".to_string(), "BINTABLE".to_string(), Some("Binary table".to_string())));
        Self {
            header: header,
            data: data,
        }
    }

    pub fn read_from_file(mut f: &mut File) -> Result<Self>  {
        //TODO: Check for mandatory words
        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        let mut columns = read_tableinfo_from_header(&header).unwrap();
        let df = read_table_bytes_to_df(&mut columns, header["NAXIS2"].value.as_int().unwrap_or(0), &mut f);
        Ok(Self::new(header, df?))
    }

    pub fn write_to_file(&mut self, mut f: &mut File) -> Result<()> {
        //TODO: This function should not repeat here and in primary hdu
        self.header.fix_header_w_mandatory_order(&MANDATORY_KEYWORDS);
        let columns = polars_to_columns(&self.data).unwrap();
        create_table_on_header(&mut self.header, &columns, self.data.height() as i64);
        
        (&mut self.header).fix_header_w_mandatory_order(&MANDATORY_KEYWORDS);
        self.header.write_to_buffer(&mut f)?;
        df_to_buffer(columns, &self.data, &mut f)?;
        Ok(())
    }
}