use core::panic;
use std::char;
use std::{fs::File, io::Read};

use crate::io::{hdus::bintable::buffer::ColumnDataBuffer, header::card::Card, utils::pad_buffer_to_fits_block, Header};
use crate::io::hdus::bintable::*;

use polars::prelude::*;
use rayon::iter::IntoParallelIterator;
use crate::io::hdus::table::table_utils::*;

extern crate num_cpus;

fn get_tform_type_size(tform: &str) -> (char, usize) {
    let tform = tform.trim();
    
    //return the last char of tform
    let tform_type = tform.chars().last().unwrap_or('A');
    let mut size = byte_value_from_str(&tform_type.to_string());
    if tform_type == 'A' {
        // The number is before the A like 48A or 8A
        size = tform[0..tform.len()-1].parse::<usize>().unwrap_or(0);
    }

    (tform_type, size)
}

#[derive(Debug)]
pub struct Column {
    pub ttype: String, 
    pub tform: String,
    pub tunit: Option<String>,
    pub tdisp: Option<String>,
    pub start_address: usize,
    pub type_bytes : usize,
    pub char_type: char,
}

pub fn byte_value_from_str(data_type : &str) -> usize {
    match data_type {
        "L" => 1,
        "X" => 1,
        "B" => 1,
        "I" => 2,
        "J" => 4,
        "K" => 8,
        "A" => 1,
        "E" => 4,
        "D" => 8,
        "C" => 8,
        "M" => 16,
        "P" => 8,
        "Q" => 16,
        _ => panic!("Wrong data type"),
    }
}

impl Column {
    pub fn new(ttype: String, tform: String, tunit: Option<String>, tdisp: Option<String>, prealloc_size: i32, start_address: usize) -> Self {
        let tform2 = tform.clone();
        let column = Column {
            ttype,
            tform,
            tunit,
            tdisp,
            start_address: start_address, 
            type_bytes: get_tform_type_size(&tform2).1,
            char_type: get_tform_type_size(&tform2).0,
        };
        column
    }
}

pub fn read_tableinfo_from_header(header: &Header) -> Result<Vec<Column>, String> {
    let mut columns: Vec<Column> = Vec::new();

    let rows = header["NAXIS2"].value.as_int().unwrap_or(0);
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

        let (_, size) = get_tform_type_size(&tform);
        let column = Column::new(ttype, tform, tunit, tdisp, rows as i32, start_address);
        
        start_address += size;
        
        columns.push(column);
    }

    Ok(columns)
}

pub fn read_table_bytes_to_df(columns : &mut Vec<Column>, nrows: i64, file: &mut File) -> Result<DataFrame, std::io::Error> {
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
    
    use rayon::prelude::*;
    //use rayon pool install
    //let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap();
    let pool = rayon::ThreadPoolBuilder::new().num_threads(n_threads as usize).build().unwrap();
    let results : Vec<Result<DataFrame, std::io::Error>> = pool.install(|| {
        limits.into_par_iter().map(|(start, end)| {
            let local_buffer: &[u8] = &buffer[start..end];
            let nbuffer_rows = (end - start) / bytes_per_row;

            let mut local_buf_cols : Vec<ColumnDataBuffer> = Vec::new();
            columns.iter().for_each(|column: &Column| {
                local_buf_cols.push(ColumnDataBuffer::new(&column.tform, nbuffer_rows as i32));
            });
            
            (0..nbuffer_rows).into_iter().for_each(|i| {
                let mut offset = 0;
                let row_start_idx = i * bytes_per_row;

                let row = &local_buffer[row_start_idx..row_start_idx + bytes_per_row];

                columns.iter().enumerate().for_each(|(j, column)| {
                    let buf_col = &mut local_buf_cols[j];
                    let col_bytes = &row[column.start_address..column.start_address + column.type_bytes];
                    buf_col.write_on_idx(col_bytes, column.char_type, i as i64);
                    offset += column.type_bytes;
                });
            });

            let df_cols = columns.iter().enumerate().map(|(i, column)| {
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
        final_df.vstack_mut(&results[i].as_ref().unwrap());
    }

    Ok(final_df)
}

pub fn polars_to_columns(df: DataFrame) -> Result<Vec<Column>, std::io::Error> {
    let mut start_address = 0;
    let mut columns : Vec<Column> = Vec::new();

    for series in df.get_columns() {
        let ttype = series.name();
        
        let tform = match series.dtype() {
            DataType::Boolean => {
                start_address += byte_value_from_str("L");
                "L".to_string()
            },
            DataType::UInt8 => {
                start_address += byte_value_from_str("X");
                "X".to_string()
            },
            DataType::Int8 => {
                start_address += byte_value_from_str("B");
                "B".to_string()
            },
            DataType::Int16 => {
                start_address += byte_value_from_str("I");
                "I".to_string()
            },
            DataType::Int32 => {
                start_address += byte_value_from_str("J");
                "J".to_string()
            },
            DataType::Int64 => {
                start_address += byte_value_from_str("K");
                "K".to_string()
            },
            DataType::Float32 => {
                start_address += byte_value_from_str("E");
                "E".to_string()
            },
            DataType::Float64 => {
                start_address += byte_value_from_str("D");
                "D".to_string()
            },
            DataType::String => {
                start_address += byte_value_from_str("A");
                
                let data = series.str().unwrap();
                let mut max_length = data.iter().map(|item| item.unwrap_or("").len()).max().unwrap();

                //Max length should be even number
                if max_length % 2 != 0 {
                    max_length += 1 as usize;
                }

                format!("{}A", max_length)
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
            0, 
            start_address
        );
        columns.push(column);
    }

    println!("Columns: {:?}", columns);
    Ok(columns)
}

pub fn calculate_number_of_bytes_of_row(columns: &Vec<Column>) -> usize {
    let mut bytes = 0;
    for column in columns.iter() {
        let (_, size) = get_tform_type_size(&column.tform);
        bytes += size;
    }
    bytes
}

pub fn create_table_on_header(header: &mut Header, columns: &Vec<Column>) {
    clear_table_on_header(header);
    let tfields = columns.len();
    let num_bytes = calculate_number_of_bytes_of_row(columns);
    header.add_card(&Card::new("TFIELDS".to_string(), tfields.to_string(), Some("Number of fields per row".to_string())));
    header.add_card(&Card::new("NAXIS1".to_string(), num_bytes.to_string(), Some("Number of bytes in row".to_string())));
    //header.add_card(&Card::new("NAXIS2".to_string(), columns[0].data.len().to_string(), Some("Number of rows".to_string())));
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

pub fn columns_to_buffer(columns: Vec<Column>, file: &mut File) -> Result<(), std::io::Error> {
    //buffer should be written in utf8
    // let rows = columns[0].data.len();
    let rows = 0;
    let bytes_per_row = calculate_number_of_bytes_of_row(&columns);
    let mut bytes_written = bytes_per_row * rows;

    // (0..nrows).flat_map( |row| {
    //     for column in columns.iter() {
    //         let (_, size) = get_tform_type_size(&column.tform);

    //         let buffer = match &column.data {
    //             Data::L(data) => data[row].to_string().into_bytes(),
    //             Data::X(data) => data[row].to_string().into_bytes(),
    //             Data::B(data) => data[row].to_be_bytes().to_vec(),
    //             Data::I(data) => data[row].to_be_bytes().to_vec(),
    //             Data::J(data) => data[row].to_be_bytes().to_vec(),
    //             Data::K(data) => data[row].to_be_bytes().to_vec(),
    //             Data::A(data) => {
    //                 let mut string = data[row].clone();
    //                 string.push_str(&" ".repeat(column.data.max_len() - data[row].len()));
    //                 string.into_bytes()
    //             },
    //             Data::E(data) => data[row].to_be_bytes().to_vec(),
    //             Data::D(data) => data[row].to_be_bytes().to_vec(),
    //             Data::C(data) => data[row].clone().into_bytes(),
    //             Data::M(data) => data[row].clone().into_bytes(),
    //             Data::P(data) => data[row].clone().into_bytes(),
    //             Data::Q(data) => data[row].clone().into_bytes(),
    //         };
        
    //         // println!("Buffer: {:?}", String::from_utf8_lossy(&buffer));

    //         //vect.par_iter().flat_map(|&item| item.to_be_bytes().to_vec()).collect::<Vec<u8>>();
    //         file.write_all(&buffer);
    //     }
    // });

    (0..rows).into_iter().for_each( |row| {
        for column in columns.iter() {
            let (_, size) = get_tform_type_size(&column.tform);

            // let buffer = match &column.data {
            //     Data::L(data) => data[row].to_string().into_bytes(),
            //     Data::X(data) => data[row].to_string().into_bytes(),
            //     Data::B(data) => data[row].to_be_bytes().to_vec(),
            //     Data::I(data) => data[row].to_be_bytes().to_vec(),
            //     Data::J(data) => data[row].to_be_bytes().to_vec(),
            //     Data::K(data) => data[row].to_be_bytes().to_vec(),
            //     Data::A(data) => {
            //         let mut string = data[row].clone();
            //         string.push_str(&" ".repeat(column.data.max_len() - data[row].len()));
            //         string.into_bytes()
            //     },
            //     Data::E(data) => data[row].to_be_bytes().to_vec(),
            //     Data::D(data) => data[row].to_be_bytes().to_vec(),
            //     Data::C(data) => data[row].clone().into_bytes(),
            //     Data::M(data) => data[row].clone().into_bytes(),
            //     Data::P(data) => data[row].clone().into_bytes(),
            //     Data::Q(data) => data[row].clone().into_bytes(),
            // };
        
            // println!("Buffer: {:?}", String::from_utf8_lossy(&buffer));

            //vect.par_iter().flat_map(|&item| item.to_be_bytes().to_vec()).collect::<Vec<u8>>();
            // file.write_all(&buffer);
        }
    });

    pad_buffer_to_fits_block(file, bytes_written)?;
    Ok(())
}
