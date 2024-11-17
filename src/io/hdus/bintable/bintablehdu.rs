use std::fs::File;
use std::io::Result;

use polars::frame::DataFrame;
use crate::io::hdus::bintable::bintable::*;

use crate::io::header::card::Card;
use crate::io::Header;

const MANDATORY_KEYWORDS: [&str; 5] = [
    "XTENSION",
    "BITPIX",
    "NAXIS",
    "NAXIS1",
    "NAXIS2",
];

/// Represents a Binary Table Header Data Unit (HDU) in a FITS file.
///
/// # Fields
/// - `header` (Header): The FITS header containing metadata for the binary table.
/// - `data` (DataFrame): The binary table's data stored as a Polars `DataFrame`.
pub struct BinTableHDU{
    pub header: Header,
    pub data: DataFrame,
}

impl BinTableHDU {
    /// Constructs a new `BinTableHDU` with the given header and data.
    ///
    /// # Arguments
    /// - `header` (Header): The FITS header containing metadata for the binary table.
    /// - `data` (DataFrame): The binary table's data.
    ///
    /// # Returns
    /// - `BinTableHDU`: A new instance of the binary table HDU.
    pub fn new(header: Header, data: DataFrame) -> Self {
        Self {
            header,
            data,
        }
    }

    /// Creates a new `BinTableHDU` with only data, initializing a basic header.
    ///
    /// # Arguments
    /// - `data` (DataFrame): The binary table's data.
    ///
    /// # Returns
    /// - `BinTableHDU`: A new instance with a basic header containing default binary table metadata.
    pub fn new_data(data: DataFrame) -> Self {
        let mut header = Header::new();
        header.add_card(&Card::new("XTENSION".to_string(), "BINTABLE".to_string(), Some("Binary table".to_string())));
        Self {
            header,
            data,
        }
    }

    /// Reads a Binary Table HDU from a FITS file.
    ///
    /// # Arguments
    /// - `f` (&mut File): The file from which to read the HDU.
    ///
    /// # Returns
    /// - `Result<BinTableHDU>`: The `BinTableHDU` containing the header and data or an I/O error.
    ///
    /// # Behavior
    /// - Reads the header to extract metadata.
    /// - Reads and parses the binary table data into a Polars `DataFrame`.
    /// - Extracts column metadata from the header for data interpretation.
    pub fn read_from_file(f: &mut File) -> Result<Self>  {
        //TODO: Check for mandatory words
        let mut header = Header::new();
        header.read_from_file(f)?;
        let mut columns = read_tableinfo_from_header(&header).unwrap();
        let df = read_table_bytes_to_df(&mut columns, &header, f);
        Ok(Self::new(header, df?))
    }

    /// Writes the Binary Table HDU to a FITS file.
    ///
    /// # Arguments
    /// - `f` (&mut File): The file to which the HDU will be written.
    ///
    /// # Returns
    /// - `Result<()>`: Returns `Ok(())` on success or an I/O error.
    ///
    /// # Behavior
    /// - Ensures the header contains mandatory keywords in the correct order.
    /// - Converts the Polars `DataFrame` into a binary buffer and writes it to the file.
    /// - Updates the header with column metadata and writes it to the file.
    pub fn write_to_file(&mut self, mut f: &mut File) -> Result<()> {
        //TODO: This function should not repeat here and in primary hdu
        self.header.fix_header_w_mandatory_order(&MANDATORY_KEYWORDS);
        let columns = polars_to_columns(&self.data).unwrap();
        create_table_on_header(&mut self.header, &columns, self.data.height() as i64);
        
        self.header.fix_header_w_mandatory_order(&MANDATORY_KEYWORDS);
        self.header.write_to_buffer(&mut f)?;
        df_to_buffer(columns, &self.data, f)?;
        Ok(())
    }
}