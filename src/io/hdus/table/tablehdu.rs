use std::fs::File;
use std::io::Result;

use polars::frame::DataFrame;
use crate::io::hdus::table::table::*;

use crate::io::header::card::Card;
use crate::io::Header;

const MANDATORY_KEYWORDS: [&str; 3] = [
    "XTENSION",
    "BITPIX",
    "NAXIS",
];

/// Represents a FITS table HDU (Header Data Unit).
///
/// # Fields
/// - `header` (Header): The FITS header containing metadata for the table.
/// - `data` (DataFrame): The Polars DataFrame holding the table's data.
pub struct TableHDU{
    pub header: Header,
    pub data: DataFrame,
}

impl TableHDU {
    /// Creates a new `TableHDU` instance with the provided header and data.
    ///
    /// # Arguments
    /// - `header` (Header): The FITS header containing metadata.
    /// - `data` (DataFrame): The table data as a Polars DataFrame.
    ///
    /// # Returns
    /// A new `TableHDU` instance initialized with the specified header and data.
    pub fn new(header: Header, data: DataFrame) -> Self {
        Self {
            header,
            data,
        }
    }

    /// Creates a new `TableHDU` instance with a default header for a binary table.
    ///
    /// # Arguments
    /// - `data` (DataFrame): The table data as a Polars DataFrame.
    ///
    /// # Returns
    /// A new `TableHDU` instance with a default binary table header and the specified data.
    pub fn new_data(data: DataFrame) -> Self {
        let mut header = Header::new();
        header.add_card(&Card::new("XTENSION".to_string(), "TABLE".to_string(), Some("Binary table".to_string())));
        Self {
            header,
            data,
        }
    }

    /// Reads a FITS table HDU from a file and constructs a `TableHDU` instance.
    ///
    /// # Arguments
    /// - `f` (&mut File): The file to read from.
    ///
    /// # Returns
    /// `Result<TableHDU, std::io::Error>`: A `TableHDU` instance containing the header and table data, or an I/O error.
    ///
    /// # Behavior
    /// - Reads the header and validates mandatory keywords.
    /// - Extracts column metadata and reads table bytes into a DataFrame.
    pub fn read_from_file(f: &mut File) -> Result<Self>  {
        //TODO: Check for mandatory words
        let mut header = Header::new();
        header.read_from_file(f)?;
        let mut columns = read_tableinfo_from_header(&header).unwrap();
        let df = read_table_bytes_to_df(&mut columns, header["NAXIS2"].value.as_int().unwrap_or(0), f);
        Ok(Self::new(header, df?))
    }

    /// Writes the `TableHDU` instance to a file.
    ///
    /// # Arguments
    /// - `f` (&mut File): The file to write to.
    ///
    /// # Returns
    /// `Result<(), std::io::Error>`: Returns `Ok(())` on success or an I/O error.
    ///
    /// # Behavior
    /// - Ensures mandatory keywords are in the correct order.
    /// - Converts the table data into FITS-compatible binary format.
    /// - Writes the header and table data to the file.
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