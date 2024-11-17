use core::panic;
use std::{fs::File, io::{Read, Write}};
use crate::io::{
    Header, 
    header::card::Card, 
    utils::pad_buffer_to_fits_block,
    utils::pad_read_buffer_to_fits_block,
};
use crate::io::hdus::table::table_utils::*;

use polars::prelude::*;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::io::hdus::table::buffer::ColumnDataBuffer;
use crate::io::hdus::bintable::split_buffer;

/// Represents a column in a FITS table.
///
/// # Fields
/// - `ttype` (String): The name of the column.
/// - `tform` (String): The format of the column (e.g., "I12", "A20").
/// - `tunit` (Option<String>): The unit of the column data, if specified.
/// - `tdisp` (Option<String>): Display format for the column, if specified.
/// - `tbcol` (Option<i32>): Starting byte of the field in a row.
/// - `start_address` (usize): The starting address of the column data within a row.
/// - `type_bytes` (usize): The number of bytes used to store the column data.
/// - `char_type` (char): A character representing the data type of the column.
#[derive(Debug)]
pub struct Column {
    pub ttype: String, 
    pub tform: String,
    pub tunit: Option<String>,
    pub tdisp: Option<String>,
    pub tbcol: Option<i32>,
    pub start_address: usize,
    pub type_bytes : usize,
    pub char_type: char,
}

impl Column {

    /// Creates a new `Column` instance.
    ///
    /// # Arguments
    /// - `ttype` (String): The name of the column.
    /// - `tform` (String): The format of the column (e.g., "I12", "A20").
    /// - `tunit` (Option<String>): The unit of the column data, if any.
    /// - `tdisp` (Option<String>): Display format for the column, if any.
    /// - `tbcol` (Option<i32>): The starting byte of the field.
    /// - `start_address` (usize): The starting address of the column data.
    ///
    /// # Returns
    /// A new `Column` instance initialized with the provided attributes.
    pub fn new(ttype: String, tform: String, tunit: Option<String>, tdisp: Option<String>, tbcol: Option<i32>, start_address: usize) -> Self {
        let tform2 = tform.clone();
        let column = Column {
            ttype,
            tform,
            tunit,
            tdisp,
            tbcol,
            start_address: start_address, 
            type_bytes: get_tform_type_size(&tform2).1,
            char_type: get_tform_type_size(&tform2).0,
        };
        column
    }
}

/// Parses the `tform` string to determine the type and size of a column.
///
/// # Arguments
/// - `tform` (str): The format string (e.g., "I12", "A20").
///
/// # Returns
/// A tuple containing:
/// - `char`: The data type (e.g., 'I', 'A').
/// - `usize`: The number of bytes used to store the column data.
///
/// # Panics
/// If the format string is invalid or cannot be parsed.
fn get_tform_type_size(tform: &str) -> (char, usize) {
    let tform = tform.trim();
    if tform.len() == 1 {
        (tform.chars().next().unwrap(), 1)
    } else {
        let (type_char, size_str) = tform.split_at(1);
        let size = size_str.split('.').next().unwrap().parse::<usize>().unwrap_or(1);
        (type_char.chars().next().unwrap(), size)
    }
}

/// Reads table metadata from a FITS header and generates a list of `Column` instances.
///
/// # Arguments
/// - `header` (&Header): The FITS header containing table information.
///
/// # Returns
/// `Result<Vec<Column>, String>`: A vector of `Column` instances or an error string if the header is invalid.
///
/// # Behavior
/// - Parses metadata like `TTYPE`, `TFORM`, and `TUNIT` for each field.
/// - Calculates start addresses and data type sizes.
pub fn read_tableinfo_from_header(header: &Header) -> Result<Vec<Column>, String> {
    let mut columns: Vec<Column> = Vec::new();
    let tfields = header["TFIELDS"].value.as_int().unwrap_or(0);

    for i in 1..=tfields {
        let ttype = header.get_card(&format!("TTYPE{}", i));
        let tform = header.get_card(&format!("TFORM{}", i));
        let tunit = header.get_card(&format!("TUNIT{}", i));
        let tdisp = header.get_card(&format!("TDISP{}", i));
        let tbcol = header.get_card(&format!("TBCOL{}", i));

        if ttype.is_none() {
            break;
        }

        let ttype = ttype.unwrap().value.to_string();
        let tform = tform.unwrap().value.to_string();
        let tunit = tunit.map(|c| c.value.to_string());
        let tdisp = tdisp.map(|c| c.value.to_string());
        let tbcol = tbcol.map(|c| c.value.to_string().parse::<i32>().unwrap());

        println!("ttype: {:?} ", ttype);
        println!("tform: {:?} ", tform);
        println!("tunit: {:?} ", tunit);
        println!("tdisp: {:?} ", tdisp);
        println!("tbcol: {:?} ", tbcol);
        println!("-------");

        let tform2 = tform.clone();
        let column = Column {
            ttype,
            tform,
            tunit,
            tdisp,
            tbcol,
            start_address: tbcol.unwrap_or(0) as usize - 1,
            type_bytes: get_tform_type_size(&tform2).1,
            char_type: get_tform_type_size(&tform2).0,
        };

        columns.push(column);
    }

    Ok(columns)
}

/// Reads table bytes from a FITS file and converts them into a Polars DataFrame.
///
/// # Arguments
/// - `columns` (&mut Vec<Column>): The list of columns describing the table structure.
/// - `nrows` (i64): The number of rows in the table.
/// - `file` (&mut File): The file from which to read the table data.
///
/// # Returns
/// `Result<DataFrame, std::io::Error>`: A DataFrame containing the table data or an I/O error.
///
/// # Behavior
/// - Uses parallel processing to read and parse table data in chunks.
/// - Converts binary data into a structured DataFrame.
pub fn read_table_bytes_to_df(columns : &mut Vec<Column>, nrows: i64, file: &mut File) -> Result<DataFrame, std::io::Error> {
    let mut n_chunks: u16 = 1;
    let mut n_threads: u16 = num_cpus::get() as u16;

    if nrows > n_threads as i64 * 10 {
        n_chunks = n_threads;
    }
    else {
        n_threads = 1;
    }
    
    let bytes_per_row = calculate_number_of_bytes_of_row(columns);
    let buffer_size = bytes_per_row * nrows as usize;
    let limits = split_buffer(buffer_size, n_chunks, bytes_per_row as u16);
    
    let mut buffer = vec![0; buffer_size];
    file.read_exact(&mut buffer)?;

    let pool = rayon::ThreadPoolBuilder::new().num_threads(n_threads as usize).build().unwrap();
    let results : Vec<Result<DataFrame, std::io::Error>> = pool.install(|| { 
        limits.into_par_iter().map(|(start, end)| {
            let local_buffer = &buffer[start as usize..end as usize];
            let nbuffer_rows = (end - start) / bytes_per_row;
            
            let mut local_buf_cols : Vec<ColumnDataBuffer> = Vec::new();
            columns.iter().for_each(|column: &Column| {
                local_buf_cols.push(ColumnDataBuffer::new(&column.tform, nbuffer_rows as i32));
            });

            (0..nbuffer_rows).into_iter().for_each(|i| {
                let row_start_idx = i * bytes_per_row;
                let row = &local_buffer[row_start_idx..row_start_idx + bytes_per_row];
                columns.iter().enumerate().for_each(|(j, column)| {
                    let (data_type, size) = get_tform_type_size(&column.tform);
                    let data = &row[column.start_address..column.start_address + column.type_bytes + 1];
                    local_buf_cols[j].write_on_idx(data, data_type, i as i64);
                });
            });

            let df_cols : Vec<Series> = columns.iter().enumerate().map(|(i, column)| {
                let buf_col = &local_buf_cols[i];
                let series = buf_col.to_series(&column.ttype);
                local_buf_cols[i].clear();
                series
            }).collect();

            let local_df = unsafe { DataFrame::new_no_checks(df_cols) };

            Ok(local_df)
        }).collect()
    });
    drop(buffer);

    let mut final_df = results[0].as_ref().unwrap().clone();
    for i in 1..results.len() {
        final_df.vstack(&results[i].as_ref().unwrap());
    }

    pad_read_buffer_to_fits_block(file, buffer_size)?;
    Ok(final_df.to_owned())
}


/// Converts a Polars DataFrame into a list of `Column` instances.
///
/// # Arguments
/// - `df` (&DataFrame): The DataFrame to be converted.
///
/// # Returns
/// `Result<Vec<Column>, std::io::Error>`: A vector of `Column` instances or an I/O error.
///
/// # Behavior
/// - Maps DataFrame column types to FITS-compatible formats.
/// - Calculates starting addresses and data type sizes.
pub fn polars_to_columns(df: &DataFrame) -> Result<Vec<Column>, std::io::Error> {
    let mut start_address : usize = 0;
    let mut sum_to_address : usize = 0;
    let mut max_length = None;

    let columns : Vec<Column> = df.get_columns().into_iter().map(|series| {
        let ttype = series.name().to_string();
        let tform = match series.dtype() {
            DataType::Int32 => {
                sum_to_address = 12 + 1;
                "I12".to_string()
            },
            DataType::Float32 => {
                sum_to_address = 15 + 1;
                "E15.7".to_string()
            },
            DataType::Float64 => {
                sum_to_address = 25 + 1;
                "D25.17".to_string()
            },
            DataType::String => {
                let data = &series.str().unwrap();
                let mut lmax_length = data.iter().map(|item| item.unwrap_or("").len()).max().unwrap();
                if lmax_length % 2 != 0 {
                    lmax_length += 1 as usize;
                }
                sum_to_address = lmax_length + 1;
                max_length = Some(lmax_length);
                format!("A{}", lmax_length)
            },
            _ => panic!("Unsupported data type"),
        };
        let mut column = Column::new(
            ttype, 
            tform, 
            None, 
            None, 
            Some(start_address as i32 + 1),
            start_address
        );

        if max_length.is_some() {
            column.type_bytes = max_length.unwrap();
            max_length = None;
        }

        start_address += sum_to_address;
        column
    }).collect();

    Ok(columns)
}

/// Calculates the total number of bytes required to store a single row of the table.
///
/// # Arguments
/// - `columns` (&Vec<Column>): The list of columns in the table.
///
/// # Returns
/// `usize`: The total number of bytes in a single row.
///
/// # Behavior
/// - Adds the sizes of all columns, including padding if necessary.
pub fn calculate_number_of_bytes_of_row(columns: &Vec<Column>) -> usize {
    let mut bytes = 0;
    for column in columns.iter() {
        let (_, size) = get_tform_type_size(&column.tform);
        println!("column {:?} ", column.ttype);
        println!("size {:?} ", size);
        bytes += size + 1;
    }
    bytes
}

/// Populates a FITS header with metadata for a table based on its columns.
///
/// # Arguments
/// - `header` (&mut Header): The FITS header to be updated.
/// - `columns` (&Vec<Column>): The list of columns describing the table structure.
/// - `nrows` (i64): The number of rows in the table.
///
/// # Behavior
/// - Adds metadata like `BITPIX`, `TFIELDS`, `NAXIS`, and `NAXISn`.
/// - Includes details for each column, such as `TTYPE` and `TFORM`.
pub fn create_table_on_header(header: &mut Header, columns: &Vec<Column>, nrows : i64) {
    clear_table_on_header(header);
    let tfields = columns.len();
    let num_bytes = calculate_number_of_bytes_of_row(columns);
    header.add_card(&Card::new("BITPIX".to_string(), 8.to_string(), Some("Table BITPIX".to_string())));
    header.add_card(&Card::new("TFIELDS".to_string(), tfields.to_string(), Some("Number of fields per row".to_string())));
    header.add_card(&Card::new("NAXIS".to_string(), 2.to_string(), Some("2D table".to_string())));
    header.add_card(&Card::new("NAXIS1".to_string(), num_bytes.to_string(), Some("Number of bytes in row".to_string())));
    header.add_card(&Card::new("NAXIS2".to_string(), nrows.to_string(), Some("Number of rows".to_string())));
    header.add_card(&Card::new("PCOUNT".to_string(), 0.to_string(), Some("Parameter count".to_string())));
    header.add_card(&Card::new("GCOUNT".to_string(), 1.to_string(), Some("Group count".to_string())));

    for (i, column) in columns.iter().enumerate() {
        header.add_card(&Card::new(format!("TTYPE{}", i + 1), column.ttype.clone(), Some("Name of field".to_string())));
        header.add_card(&Card::new(format!("TFORM{}", i + 1), column.tform.clone(), Some("Format of field".to_string())));
        if let Some(tunit) = &column.tunit {
            header.add_card(&Card::new(format!("TUNIT{}", i + 1), tunit.clone(), Some("Unit of field".to_string())));
        }
        if let Some(tdisp) = &column.tdisp {
            header.add_card(&Card::new(format!("TDISP{}", i + 1), tdisp.clone(), Some("Display format of field".to_string())));
        }
        if let Some(tbcol) = &column.tbcol {
            //TBCOL is the start byte of the field
            header.add_card(&Card::new(format!("TBCOL{}", i + 1), tbcol.to_string(), Some("Starting byte of field".to_string())));
        }
    }
}

/// Converts a DataFrame into a FITS table binary buffer and writes it to a file.
///
/// # Arguments
/// - `columns` (Vec<Column>): The list of columns describing the table structure.
/// - `df` (&DataFrame): The DataFrame containing the table data.
/// - `file` (&mut File): The file to which the binary buffer will be written.
///
/// # Returns
/// `Result<(), std::io::Error>`: Returns `Ok(())` on success or an I/O error.
///
/// # Behavior
/// - Converts DataFrame rows into binary format based on column types.
/// - Pads the buffer to a FITS-compliant block size before writing.
pub fn df_to_buffer(columns: Vec<Column>, df: &DataFrame, file: &mut File) -> Result<(), std::io::Error> {
    //buffer should be written in utf8
    let nrows = df.height();
    let bytes_per_row = calculate_number_of_bytes_of_row(&columns);

    let mut n_chunks: u16 = 1;
    let mut n_threads: u16 = num_cpus::get() as u16;

    if nrows > n_threads as usize * 10 {
        n_chunks = n_threads;
    }
    else {
        n_threads = 1;
    }

    let limits = split_buffer(nrows, n_chunks, bytes_per_row as u16);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(n_threads as usize).build().unwrap();

    let bufs : Vec<Vec<u8>> = pool.install(|| {
        limits.into_iter().map(|(start, end)| {
            let nbuffer_rows = end - start;
            let local_df = &df.slice(start as i64, nbuffer_rows);
            let mut local_buffer = vec![0x00; nbuffer_rows * bytes_per_row];

            columns.iter().for_each(|column| {
                let series = &local_df.column(&column.ttype).unwrap();
                let (_, size) = get_tform_type_size(&column.tform);

                match series.dtype() {
                    
                    DataType::Int32 => {
                        series.i32()
                            .expect("Expected an i32 series")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let item = item.unwrap();
                                let mut string = item.to_string();
                                while string.len() <= column.type_bytes {
                                    string.push(' ');
                                }

                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes + 1].copy_from_slice(&string.as_bytes());
                            });
                    },
                    DataType::Float32 => {
                        series.f32()
                            .expect("Expected a f32 series")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let item = item.unwrap();
                                let mut string = format_scientific(item as f64, size);
                                while string.len() <= column.type_bytes {
                                    string.push(' ');
                                }
                                println!("column.start_address: {:?} ", column.start_address);
                                println!("column.type_bytes: {:?} ", column.type_bytes);
                                println!("column.ttype: {:?} ", column.ttype);
                                println!("column.tform: {:?} ", column.tform);
                                println!("buffer len {:?} ", string.len());

                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes + 1].copy_from_slice(&string.as_bytes());
                            });
                    },
                    DataType::Float64 => {
                        series.f64()
                            .expect("Expected a f64 series")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let item = item.unwrap();
                                let mut string = format_scientific(item, size);
                                while string.len() <= column.type_bytes {
                                    string.push(' ');
                                }

                                println!("column.start_address: {:?} ", column.start_address);
                                println!("column.type_bytes: {:?} ", column.type_bytes);
                                println!("column.ttype: {:?} ", column.ttype);
                                println!("column.tform: {:?} ", column.tform);
                                println!("buffer len {:?} ", string.len());

                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes + 1].copy_from_slice(&string.as_bytes());
                            });
                    },
                    DataType::String => {
                        series.cast(&DataType::String).unwrap().str()
                            .expect("Expected a string series")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let item = item.unwrap_or("");
                                let mut buffer = item.as_bytes().to_vec();
                                
                                while buffer.len() <= column.type_bytes {
                                    buffer.push(b' ');
                                }

                                println!("column.start_address: {:?} ", column.start_address);
                                println!("column.type_bytes: {:?} ", column.type_bytes);
                                println!("column.ttype: {:?} ", column.ttype);
                                println!("column.tform: {:?} ", column.tform);
                                println!("buffer len {:?} ", buffer.len());

                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes + 1].copy_from_slice(&buffer);
                            });
                    },
                    _ => panic!("Unsupported data type"),
                }
            });
            local_buffer.to_owned()
        }).collect()
    });

    let mut bytes_written = 0;
    bufs.into_iter().for_each(|buf| {
        bytes_written += file.write(&buf).unwrap();
    });

    if bytes_written < nrows as usize * bytes_per_row {
        panic!("Error writing to file");
    }

    pad_buffer_to_fits_block(file, bytes_written)?;
    Ok(())
}

