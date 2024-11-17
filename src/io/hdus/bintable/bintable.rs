use core::panic;
use std::io::Write;
use std::{fs::File, io::Read};

use crate::io::{hdus::bintable::buffer::Buffer, header::card::Card, utils::pad_buffer_to_fits_block, utils::pad_read_buffer_to_fits_block, Header};
use crate::io::hdus::bintable::*;

use polars::prelude::*;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use crate::io::hdus::table::table_utils::*;

extern crate num_cpus;


/// Represents a column in a binary FITS table.
///
/// # Fields
/// - `ttype` (String): The name of the column.
/// - `tform` (String): The format of the column's data.
/// - `tunit` (Option<String>): The unit of the column's data, if available.
/// - `tdisp` (Option<String>): The display format of the column's data, if specified.
/// - `start_address` (usize): The starting byte offset for the column's data in each row.
/// - `type_bytes` (usize): The number of bytes required to store the column's data.
/// - `type_letter` (String): The first letter of the column's format, indicating the data type.
#[derive(Debug)]
pub struct Column {
    pub ttype: String, 
    pub tform: String,
    pub tunit: Option<String>,
    pub tdisp: Option<String>,
    pub start_address: usize,
    pub type_bytes : usize,
    pub type_letter: String,
}

impl Column {
    /// Constructs a new `Column` instance with the specified metadata.
    ///
    /// # Arguments
    /// - `ttype` (String): The name of the column.
    /// - `tform` (String): The format of the column's data.
    /// - `tunit` (Option<String>): The unit of the column's data, if available.
    /// - `tdisp` (Option<String>): The display format of the column's data, if specified.
    /// - `start_address` (usize): The starting byte offset for the column's data in each row.
    ///
    /// # Returns
    /// A new `Column` instance with calculated `type_bytes` and `type_letter`.
    pub fn new(ttype: String, tform: String, tunit: Option<String>, tdisp: Option<String>, start_address: usize) -> Self {
        let tform2 = tform.clone();
        let column = Column {
            ttype,
            tform,
            tunit,
            tdisp,
            start_address: start_address, 
            type_bytes: get_data_bytes_size(&tform2),
            type_letter: get_first_letter(&tform2).to_string(),
        };
        column
    }
}

/// Reads column information from a FITS header and constructs a vector of `Column` instances.
///
/// # Arguments
/// - `header` (&Header): The FITS header containing column metadata.
///
/// # Returns
/// - `Result<Vec<Column>, String>`: A vector of `Column` instances or an error string.
///
/// # Behavior
/// - Extracts column properties like `TTYPE`, `TFORM`, `TUNIT`, and `TDISP` from the header.
/// - Computes the starting address and byte size for each column.
pub fn read_tableinfo_from_header(header: &Header) -> Result<Vec<Column>, String> {
    let mut columns: Vec<Column> = Vec::new();

    let tfields = header["TFIELDS"].value.as_int().unwrap_or(0);
    let mut start_address = 0;

    for i in 1..=tfields {
        let ttype: Option<&Card> = header.get_card(&format!("TTYPE{}", i));
        let tform: Option<&Card> = header.get_card(&format!("TFORM{}", i));
        let tunit: Option<&Card> = header.get_card(&format!("TUNIT{}", i));
        let tdisp: Option<&Card> = header.get_card(&format!("TDISP{}", i));

        if ttype.is_none() {
            break;
        }

        let ttype: String = ttype.unwrap().value.to_string().trim_end().to_string();
        let tform: String = tform.unwrap().value.to_string();
        let tunit: Option<String> = tunit.map(|c| c.value.to_string());
        let tdisp: Option<String> = tdisp.map(|c| c.value.to_string());

        let size = get_data_bytes_size(&tform);
        let column = Column::new(ttype, tform, tunit, tdisp, start_address);

        start_address += size;

        columns.push(column);
    }

    Ok(columns)
}

/// Reads binary table data from a file and converts it into a Polars `DataFrame`.
///
/// # Arguments
/// - `columns` (&mut Vec<Column>): A mutable reference to a vector of `Column` instances.
/// - `header` (&Header): The FITS header containing metadata for the table.
/// - `file` (&mut File): The file from which to read the data.
///
/// # Returns
/// - `Result<DataFrame, std::io::Error>`: The resulting `DataFrame` or an I/O error.
///
/// # Behavior
/// - Reads the table's binary data in chunks using multiple threads.
/// - Constructs a `DataFrame` by iterating over the data rows and columns.
pub fn read_table_bytes_to_df(columns : &mut Vec<Column>, header: &Header, file: &mut File) -> Result<DataFrame, std::io::Error> {
    let nrows = header["NAXIS2"].value.as_int().unwrap_or(0);
    let mut n_chunks: u16 = 1;
    let mut n_threads: u16 = num_cpus::get() as u16;

    if nrows > n_threads as i64 * 10 {
        n_chunks = n_threads;
    }
    else {
        n_threads = 1;
    }

    let bytes_per_row = calculate_number_of_bytes_of_row(&columns);
    let buffer_size = nrows as usize * bytes_per_row;
    let limits = split_buffer(buffer_size, n_chunks, bytes_per_row as u16);

    let mut buffer = vec![0; buffer_size];
    file.read_exact(&mut buffer)?;
    
    //TODO read suplemental data here
    if header.get_card("THEAP").is_some(){
        // Skip theap bytes
        let theap = header.get_card("THEAP").unwrap().value.as_int().unwrap();
        let mut theap_buffer = vec![0; theap as usize];
        file.read_exact(&mut theap_buffer)?;
        drop(theap_buffer);
    }
    let mut pcount_buffer;
    if header.get_card("PCOUNT").is_some(){
        // Skip pcount bytes
        let pcount = header.get_card("PCOUNT").unwrap().value.as_int().unwrap();
        pcount_buffer = vec![0; pcount as usize];
        file.read_exact(&mut pcount_buffer)?;
    }


    //use rayon pool install
    //let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap();
    let pool = rayon::ThreadPoolBuilder::new().num_threads(n_threads as usize).build().unwrap();
    let results : Vec<Result<DataFrame, std::io::Error>> = pool.install(|| {
        limits.into_par_iter().map(|(start, end)| {
            let local_buffer: &[u8] = &buffer[start..end];
            let nbuffer_rows = (end - start) / bytes_per_row;

            let mut local_buf_cols : Vec<Buffer> = Vec::new();
            columns.iter().for_each(|column: &Column| {
                local_buf_cols.push(Buffer::new(&column.tform, nbuffer_rows as i32));
            });
            
            (0..nbuffer_rows).into_iter().for_each(|i| {
                let row_start_idx = i * bytes_per_row;
                let row = &local_buffer[row_start_idx..row_start_idx + bytes_per_row];
                columns.iter().enumerate().for_each(|(j, column)| {
                    let buf_col = &mut local_buf_cols[j];
                    let col_bytes = &row[column.start_address..column.start_address + column.type_bytes];
                    buf_col.write_on_idx(col_bytes, i as i64);                });
            });

            let df_cols = columns.iter().enumerate().map(|(i, column)| {
                if (get_first_letter(&column.tform) == "P") | (get_first_letter(&column.tform) == "Q") {
                    &local_buf_cols[i].read_var_len_cols();
                }
                
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
        final_df.vstack_mut(&results[i].as_ref().unwrap()).unwrap();
    }
    pad_read_buffer_to_fits_block(file, buffer_size)?;

    Ok(final_df)
}

/// Converts a Polars `DataFrame` into a vector of `Column` instances.
///
/// # Arguments
/// - `df` (&DataFrame): The input `DataFrame`.
///
/// # Returns
/// - `Result<Vec<Column>, std::io::Error>`: A vector of `Column` instances or an I/O error.
///
/// # Behavior
/// - Maps each column in the `DataFrame` to a `Column` with the corresponding FITS-compatible format.
/// - Handles primitive types, strings, and list types with appropriate `TFORM` and byte size calculations.
pub fn polars_to_columns(df: &DataFrame) -> Result<Vec<Column>, std::io::Error> {
    let mut start_address = 0;
    let mut sum_to_address = 0;
    
    let columns : Vec<Column> = df.get_columns().into_iter().map(|series| {
        let ttype = series.name();
        let tform = match series.dtype() {
            DataType::Boolean => {
                sum_to_address = byte_value_from_str("L");
                "L".to_string()
            },
            DataType::UInt8 => {
                sum_to_address = byte_value_from_str("X");
                "X".to_string()
            },
            DataType::Int8 => {
                sum_to_address = byte_value_from_str("B");
                "B".to_string()
            },
            DataType::Int16 => {
                sum_to_address = byte_value_from_str("I");
                "I".to_string()
            },
            DataType::Int32 => {
                sum_to_address = byte_value_from_str("J");
                "J".to_string()
            },
            DataType::Int64 => {
                sum_to_address = byte_value_from_str("K");
                "K".to_string()
            },
            DataType::Float32 => {
                sum_to_address = byte_value_from_str("E");
                "E".to_string()
            },
            DataType::Float64 => {
                sum_to_address = byte_value_from_str("D");
                "D".to_string()
            },
            DataType::String => {
                let data = &series.str().unwrap();
                let mut max_length = data.iter().map(|item| item.unwrap_or("").len()).max().unwrap();

                //Max length should be even number
                if max_length % 2 != 0 {
                    max_length += 1 as usize;
                }
                sum_to_address = max_length;
                format!("{}A", max_length)
            },
            DataType::List(dtype ) => {
                // Get list shape 
                let list_shape = series.list().unwrap().get(0).unwrap().len();
                
                match dtype.to_physical() {
                    DataType::Float64 => {
                        sum_to_address = list_shape * byte_value_from_str("D");
                        format!("{}D", list_shape)
                    },
                    DataType::Float32 => {
                        sum_to_address = list_shape * byte_value_from_str("E");
                        format!("{}E", list_shape)
                    },
                    DataType::Int64 => {
                        sum_to_address = list_shape * byte_value_from_str("K");
                        format!("{}K", list_shape)
                    },
                    DataType::Int32 => {
                        sum_to_address = list_shape * byte_value_from_str("J");
                        format!("{}J", list_shape)
                    },
                    DataType::Int16 => {
                        sum_to_address = list_shape * byte_value_from_str("I");
                        format!("{}I", list_shape)
                    },
                    DataType::Int8 => {
                        sum_to_address = list_shape * byte_value_from_str("B");
                        format!("{}B", list_shape)
                    },
                    DataType::UInt8 => {
                        sum_to_address = list_shape * byte_value_from_str("X");
                        format!("{}X", list_shape)
                    },
                    DataType::Boolean => {
                        sum_to_address = list_shape * byte_value_from_str("L");
                        format!("{}L", list_shape)
                    },
                    _ => {
                        panic!("Unsupported data type for array column");
                    }
                }

            },
            _ => {
                panic!("Unsupported data type");
            }
        };
        
        let column: Column = Column::new(
            ttype.to_string(), 
            tform, 
            None, 
            None, 
            start_address
        );

        start_address += sum_to_address;
        column
    }).collect();

    Ok(columns)
}

/// Calculates the total number of bytes required to store one row of the binary table.
///
/// # Arguments
/// - `columns` (&Vec<Column>): The vector of `Column` instances representing the table's structure.
///
/// # Returns
/// - `usize`: The total number of bytes per row.
pub fn calculate_number_of_bytes_of_row(columns: &Vec<Column>) -> usize {
    let mut bytes = 0;
    for column in columns.iter() {
        let size = get_data_bytes_size(&column.tform);
        bytes += size;
    }
    bytes
}

/// Creates or updates the FITS header with metadata for a binary table.
///
/// # Arguments
/// - `header` (&mut Header): The FITS header to update.
/// - `columns` (&Vec<Column>): The vector of `Column` instances representing the table's structure.
/// - `nrows` (i64): The number of rows in the table.
///
/// # Behavior
/// - Adds mandatory header keywords like `XTENSION`, `NAXIS1`, and `NAXIS2`.
/// - Populates column-specific keywords (`TTYPE`, `TFORM`, `TUNIT`, etc.) for each column.
pub fn create_table_on_header(header: &mut Header, columns: &Vec<Column>, nrows: i64) {
    clear_table_on_header(header);
    let tfields = columns.len();
    let num_bytes = calculate_number_of_bytes_of_row(columns);
    header.add_card_on_index(&Card::new("XTENSION".to_string(), "BINTABLE".to_string(), Some("Binary table".to_string())), 0);
    header.add_card_after(&Card::new("BITPIX".to_string(), 8.to_string(), Some("Table BITPIX".to_string())), "XTENSION");
    header.add_card_after(&Card::new("NAXIS".to_string(), 2.to_string(), Some("2D table".to_string())), "BITPIX");
    header.add_card_after(&Card::new("NAXIS1".to_string(), num_bytes.to_string(), Some("Number of bytes in row".to_string())), "NAXIS");
    header.add_card_after(&Card::new("NAXIS2".to_string(), nrows.to_string(), Some("Number of rows".to_string())), "NAXIS1");
    header.add_card_after(&Card::new("PCOUNT".to_string(), 0.to_string(), Some("Parameter count".to_string())), "NAXIS2");
    header.add_card_after(&Card::new("GCOUNT".to_string(), 1.to_string(), Some("Group count".to_string())), "PCOUNT");
    header.add_card_after(&Card::new("TFIELDS".to_string(), tfields.to_string(), Some("Number of fields per row".to_string())), "GCOUNT");
    for (i, column) in columns.iter().enumerate() {
        header.add_card(&Card::new(format!("TTYPE{}", i + 1), column.ttype.clone(), Some("Name of field".to_string())));
        header.add_card(&Card::new(format!("TFORM{}", i + 1), column.tform.clone(), Some("Format of field".to_string())));
        if let Some(tunit) = &column.tunit {
            header.add_card(&Card::new(format!("TUNIT{}", i + 1), tunit.clone(), Some("Unit of field".to_string())));
        }
        if let Some(tdisp) = &column.tdisp {
            header.add_card(&Card::new(format!("TDISP{}", i + 1), tdisp.clone(), Some("Display format of field".to_string())));
        }
    }
}

/// Converts a Polars `DataFrame` into a binary buffer and writes it to a file.
///
/// # Arguments
/// - `columns` (Vec<Column>): The vector of `Column` instances representing the table's structure.
/// - `df` (&DataFrame): The `DataFrame` containing the table's data.
/// - `file` (&mut File): The file to which the binary buffer is written.
///
/// # Returns
/// - `Result<(), std::io::Error>`: Returns `Ok(())` on success or an I/O error.
///
/// # Behavior
/// - Iterates over rows and columns of the `DataFrame`, converting data into binary format.
/// - Supports parallel processing for efficiency.
/// - Ensures the final buffer size is padded to the nearest FITS block.
pub fn df_to_buffer(columns: Vec<Column>, df: &DataFrame, file: &mut File) -> Result<(), std::io::Error> {
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

    let limits = split_buffer(nrows, n_chunks, 1);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(n_threads as usize).build().unwrap();
    let bufs : Vec<Vec<u8>> = pool.install(|| {
        limits.into_par_iter().map(|(start, end)| {
            let nbuffer_rows: usize = end - start;
            let local_df = &df.slice(start as i64, nbuffer_rows);
            let mut local_buffer : Vec<u8> = vec![0x00; nbuffer_rows * bytes_per_row];

            columns.iter().for_each(|column| {
                let series = &local_df.column(&column.ttype).unwrap();

                match series.dtype() {
                    DataType::Boolean => {
                        series.bool()
                            .expect("series was not an bool dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let bytes = item.unwrap() as u8;
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&[bytes]);
                            });
                    },
                    DataType::UInt8 => {
                        series.u8()
                            .expect("series was not an u8 dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let bytes = item.unwrap() as u8;
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&[bytes]);
                            });
                    },
                    DataType::Int8 => {
                        series.i8()
                            .expect("series was not an i8 dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let bytes = item.unwrap() as i8;
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&bytes.to_be_bytes());
                            });
                    },
                    DataType::Int16 => {
                        series.i16()
                            .expect("series was not an i16 dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let bytes = item.unwrap() as i16;
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&bytes.to_be_bytes());
                            });
                    },
                    DataType::Int32 => {
                        series.i32()
                            .expect("series was not an i32 dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let bytes = item.unwrap() as i32;
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&bytes.to_be_bytes());
                            });
                    },
                    DataType::Int64 => {
                        series.i64()
                            .expect("series was not an i64 dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let bytes = item.unwrap() as i64;
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&bytes.to_be_bytes());
                            });
                    },
                    DataType::Float32 => {
                        series.f32()
                            .expect("series was not an f32 dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let bytes = item.unwrap() as f32;
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&bytes.to_be_bytes());
                            });
                    },
                    DataType::Float64 => {
                        series.f64()
                            .expect("series was not an f64 dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let bytes = item.unwrap() as f64;
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&bytes.to_be_bytes());
                            });
                    },
                    DataType::List(dtype) => {
                        match dtype.to_physical() {
                            DataType::Boolean => {
                                series.list()
                                    .expect("series was not an list dtype")
                                    .into_iter()
                                    .enumerate()
                                    .for_each(|(j, item)| {
                                        let array = item.unwrap();
                                        let row_start_add = j * bytes_per_row;
                                        let col_start_add = row_start_add + column.start_address;
                                        
                                        array.iter().enumerate().for_each(|(k, it )| {
                                            if let AnyValue::Boolean(item) = it {
                                                let bytes = item as u8;
                                                local_buffer[col_start_add + k..col_start_add + k + 1].copy_from_slice(&[bytes]);
                                            }
                                        });
                                    });
                            },
                            DataType::UInt8 => {
                                series.list()
                                    .expect("series was not an list dtype")
                                    .into_iter()
                                    .enumerate()
                                    .for_each(|(j, item)| {
                                        let array = item.unwrap();
                                        let row_start_add = j * bytes_per_row;
                                        let col_start_add = row_start_add + column.start_address;
                                        
                                        array.iter().enumerate().for_each(|(k, it )| {
                                            if let AnyValue::UInt8(item) = it {
                                                let bytes = item as u8;
                                                local_buffer[col_start_add + k..col_start_add + k + 1].copy_from_slice(&[bytes]);
                                            }
                                        });
                                    });
                            },
                            DataType::Int8 => {
                                series.list()
                                    .expect("series was not an list dtype")
                                    .into_iter()
                                    .enumerate()
                                    .for_each(|(j, item)| {
                                        let array = item.unwrap();
                                        let row_start_add = j * bytes_per_row;
                                        let col_start_add = row_start_add + column.start_address;
                                        
                                        array.iter().enumerate().for_each(|(k, it )| {
                                            if let AnyValue::Int8(item) = it {
                                                let bytes = item as i8;
                                                local_buffer[col_start_add + k..col_start_add + k + 1].copy_from_slice(&bytes.to_be_bytes());
                                            }
                                        });
                                    });
                            },
                            DataType::Int16 => {
                                series.list()
                                    .expect("series was not an list dtype")
                                    .into_iter()
                                    .enumerate()
                                    .for_each(|(j, item)| {
                                        let array = item.unwrap();
                                        let row_start_add = j * bytes_per_row;
                                        let col_start_add = row_start_add + column.start_address;
                                        
                                        array.iter().enumerate().for_each(|(k, it )| {
                                            if let AnyValue::Int16(item) = it {
                                                let bytes = item as i16;
                                                local_buffer[col_start_add + k * 2..col_start_add + k * 2 + 2].copy_from_slice(&bytes.to_be_bytes());
                                            }
                                        });
                                    });
                            },
                            DataType::Int32 => {
                                series.list()
                                    .expect("series was not an list dtype")
                                    .into_iter()
                                    .enumerate()
                                    .for_each(|(j, item)| {
                                        let array = item.unwrap();
                                        let row_start_add = j * bytes_per_row;
                                        let col_start_add = row_start_add + column.start_address;
                                        
                                        array.iter().enumerate().for_each(|(k, it )| {
                                            if let AnyValue::Int32(item) = it {
                                                let bytes = item as i32;
                                                local_buffer[col_start_add + k * 4..col_start_add + k * 4 + 4].copy_from_slice(&bytes.to_be_bytes());
                                            }
                                        });
                                    });
                            },
                            DataType::Int64 => {
                                series.list()
                                    .expect("series was not an list dtype")
                                    .into_iter()
                                    .enumerate()
                                    .for_each(|(j, item)| {
                                        let array = item.unwrap();
                                        let row_start_add = j * bytes_per_row;
                                        let col_start_add = row_start_add + column.start_address;
                                        
                                        array.iter().enumerate().for_each(|(k, it )| {
                                            if let AnyValue::Int64(item) = it {
                                                let bytes = item as i64;
                                                local_buffer[col_start_add + k * 8..col_start_add + k * 8 + 8].copy_from_slice(&bytes.to_be_bytes());
                                            }
                                        });
                                    });
                            },
                            DataType::Float32 => {
                                series.list()
                                    .expect("series was not an list dtype")
                                    .into_iter()
                                    .enumerate()
                                    .for_each(|(j, item)| {
                                        let array = item.unwrap();
                                        let row_start_add = j * bytes_per_row;
                                        let col_start_add = row_start_add + column.start_address;
                                        
                                        array.iter().enumerate().for_each(|(k, it )| {
                                            if let AnyValue::Float32(item) = it {
                                                let bytes = item as f32;
                                                local_buffer[col_start_add + k * 4..col_start_add + k * 4 + 4].copy_from_slice(&bytes.to_be_bytes());
                                            }
                                        });
                                    });
                            },
                            DataType::Float64 => {
                                series.list()
                                    .expect("series was not an list dtype")
                                    .into_iter()
                                    .enumerate()
                                    .for_each(|(j, item)| {
                                        let array = item.unwrap();
                                        let row_start_add = j * bytes_per_row;
                                        let col_start_add = row_start_add + column.start_address;
                                        
                                        array.iter().enumerate().for_each(|(k, it )| {
                                            if let AnyValue::Float64(item) = it {
                                                let bytes = item as f64;
                                                local_buffer[col_start_add + k * 8..col_start_add + k * 8 + 8].copy_from_slice(&bytes.to_be_bytes());
                                            }
                                        });
                                    });
                            },
                            _ => panic!("Unsupported data type for array column"),
                        }
                    }
                    DataType::String => {
                        series.str()
                            .expect("series was not an string dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let string = item.unwrap();
                                let mut string = string.to_string();
                                string.push_str(&" ".repeat(column.type_bytes - string.len()));
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&string.into_bytes());
                            });
                    },
                    _ => {
                        series.str()
                            .expect("series was not an string dtype")
                            .into_iter()
                            .enumerate()
                            .for_each(|(j, item)| {
                                let string = item.unwrap();
                                let mut string = string.to_string();
                                string.push_str(&" ".repeat(column.type_bytes - string.len()));
                                let row_start_add = j * bytes_per_row;
                                let col_start_add = row_start_add + column.start_address;
                                local_buffer[col_start_add..col_start_add + column.type_bytes].copy_from_slice(&string.into_bytes());
                            });
                    }
                    
                }
                
            });
            
            
            local_buffer.to_owned()
        }).collect()
    });

    let mut bytes_written = 0;
    bufs.into_iter().for_each(|buffer| {
        bytes_written += file.write(&buffer).unwrap();
    });
    
    if bytes_written < nrows as usize * bytes_per_row {
        panic!("Error writing to file");
    }

    pad_buffer_to_fits_block(file, bytes_written)?;
    Ok(())
}
