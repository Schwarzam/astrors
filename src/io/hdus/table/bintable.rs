use core::panic;
use std::{fs::File, io::{Read, Write}};

use crate::io::{Header, header::card::Card, utils::pad_buffer_to_fits_block};
use polars::prelude::*; // Polars library

#[derive(Debug, PartialEq)]
pub enum Data {
    L(Vec<bool>), // Logical
    X(Vec<u8>), // Bit
    B(Vec<i8>), // Byte
    I(Vec<i16>), // Short
    J(Vec<i32>), // Int
    K(Vec<i64>), // Long
    A(Vec<String>), // Char
    E(Vec<f32>), // Float
    D(Vec<f64>), // Double
    C(Vec<String>), // Complex
    M(Vec<String>), // Double complex
    P(Vec<String>), // Array descriptor
    Q(Vec<String>), // Array descriptor
}



impl Data {
    pub fn new(tform : &str) -> Self {
        let tform = tform.trim();
        let tform_type = tform.chars().last().unwrap_or('A');
        
        match tform_type {
            'L' => Data::L(Vec::new()),
            'X' => Data::X(Vec::new()),
            'B' => Data::B(Vec::new()),
            'I' => Data::I(Vec::new()),
            'J' => Data::J(Vec::new()),
            'K' => Data::K(Vec::new()),
            'A' => Data::A(Vec::new()),
            'E' => Data::E(Vec::new()),
            'D' => Data::D(Vec::new()),
            'C' => Data::C(Vec::new()),
            'M' => Data::M(Vec::new()),
            'P' => Data::P(Vec::new()),
            'Q' => Data::Q(Vec::new()),
            _ => Data::A(Vec::new()),
        }
    }

    pub fn byte_value(&self) -> usize{
        match self {
            Data::L(_) => 1,
            Data::X(_) => 1,
            Data::B(_) => 1,
            Data::I(_) => 2,
            Data::J(_) => 4,
            Data::K(_) => 8,
            Data::A(_) => 1,
            Data::E(_) => 4,
            Data::D(_) => 8,
            Data::C(_) => 8,
            Data::M(_) => 16,
            Data::P(_) => 8,
            Data::Q(_) => 16,
        }
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

    pub fn push(&mut self, bytes: Vec<u8>, data_type: char) {
        match data_type {
            'L' => {
                // parse bytes to bool
                match self {
                    Data::L(data) => data.push(bytes[0] != 0),
                    _ => panic!("Wrong data type"),
                }
            }
            'X' => {
                // parse bytes to u8
                match self {
                    Data::X(data) => data.push(bytes[0]),
                    _ => panic!("Wrong data type"),
                }
            }
            'B' => {
                // parse bytes to i8
                match self {
                    Data::B(data) => data.push(bytes[0] as i8),
                    _ => panic!("Wrong data type"),
                }
            }
            'I' => {
                // parse bytes to i16
                match self {
                    Data::I(data) => data.push(i16::from_be_bytes([bytes[0], bytes[1]])),
                    _ => panic!("Wrong data type"),
                }
            }
            'J' => {
                // parse bytes to i32
                match self {
                    Data::J(data) => data.push(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
                    _ => panic!("Wrong data type"),
                }
            }
            'K' => {
                // parse bytes to i64
                match self {
                    Data::K(data) => data.push(i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]])),
                    _ => panic!("Wrong data type"),
                }
            }
            'A' => {
                // parse bytes to String
                match self {
                    Data::A(data) => {
                        let mut string = String::new();
                        for byte in bytes {
                            string.push(byte as char);
                        }
                        println!("String: {}", string);
                        data.push(string);
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            'E' => {
                // parse bytes to f32
                match self {
                    Data::E(data) => data.push(f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
                    _ => panic!("Wrong data type"),
                }
            }
            'D' => {
                // parse bytes to f64
                match self {
                    Data::D(data) => data.push(f64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]])),
                    _ => panic!("Wrong data type"),
                }
            }
            'C' => {
                // parse bytes to String
                match self {
                    Data::C(data) => {
                        let mut string = String::new();
                        for byte in bytes {
                            string.push(byte as char);
                        }
                        data.push(string);
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            'M' => {
                // parse bytes to String
                match self {
                    Data::M(data) => {
                        let mut string = String::new();
                        for byte in bytes {
                            string.push(byte as char);
                        }
                        data.push(string);
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            'P' => {
                // parse bytes to String
                match self {
                    Data::P(data) => {
                        let mut string = String::new();
                        for byte in bytes {
                            string.push(byte as char);
                        }
                        data.push(string);
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            'Q' => {
                // parse bytes to String
                match self {
                    Data::Q(data) => {
                        let mut string = String::new();
                        for byte in bytes {
                            string.push(byte as char);
                        }
                        data.push(string);
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            _ => panic!("Wrong data type"),

        }
    }

    pub fn len(&self) -> usize {
        match self {
            Data::L(data) => data.len(),
            Data::X(data) => data.len(),
            Data::B(data) => data.len(),
            Data::I(data) => data.len(),
            Data::J(data) => data.len(),
            Data::K(data) => data.len(),
            Data::A(data) => data.len(),
            Data::E(data) => data.len(),
            Data::D(data) => data.len(),
            Data::C(data) => data.len(),
            Data::M(data) => data.len(),
            Data::P(data) => data.len(),
            Data::Q(data) => data.len(),
        }
    }

    //no need for max_len on bintable

}

fn get_tform_type_size(tform: &str) -> (char, usize) {
    let tform = tform.trim();
    
    //return the last char of tform
    let tform_type = tform.chars().last().unwrap_or('A');
    let mut size = Data::byte_value_from_str(&tform_type.to_string());
    if tform_type == 'A' {

        // The number is before the A like 48A or 8A
        size = tform[0..tform.len()-1].parse::<usize>().unwrap_or(0);
        
    }

    (tform_type, size)
}

#[derive(Debug)]
pub struct Column {
    ttype: String, 
    tform: String,
    tunit: Option<String>,
    tdisp: Option<String>,
    data: Data,
}

pub fn read_tableinfo_from_header(header: &Header) -> Result<Vec<Column>, String> {
    let mut columns: Vec<Column> = Vec::new();
    let tfields = header["NAXIS2"].value.as_int().unwrap_or(0);

    for i in 1..=tfields {
        let ttype = header.get_card(&format!("TTYPE{}", i));
        let tform = header.get_card(&format!("TFORM{}", i));
        let tunit = header.get_card(&format!("TUNIT{}", i));
        let tdisp = header.get_card(&format!("TDISP{}", i));

        if ttype.is_none() {
            break;
        }

        let ttype = ttype.unwrap().value.to_string();
        let tform = tform.unwrap().value.to_string();
        let tunit = tunit.map(|c| c.value.to_string());
        let tdisp = tdisp.map(|c| c.value.to_string());

        let tform2 = tform.clone();
        let column = Column {
            ttype,
            tform,
            tunit,
            tdisp,
            data : Data::new(&tform2),
        };

        columns.push(column);
    }

    Ok(columns)
}

pub fn fill_columns_w_data(columns : &mut Vec<Column>, nrows: i64, file: &mut File) -> Result<(), std::io::Error> {
    for row in 1..=nrows{
        for column in columns.iter_mut() {
            let (data_type, size) = get_tform_type_size(&column.tform);
    
            let mut buffer = vec![0; size];
            file.read_exact(&mut buffer)?;
            
            column.data.push(buffer, data_type);
        }
    }
    Ok(())
}

pub fn columns_to_polars(columns: Vec<Column>) -> Result<DataFrame, String> {
    let mut polars_columns: Vec<Series> = Vec::new();
    for column in columns {
        let series = match column.data {
            Data::L(data) => Series::new(&column.ttype, data),
            Data::X(data) => panic!("Bit column not supported"),
            Data::B(data) => Series::new(&column.ttype, data),
            Data::I(data) => Series::new(&column.ttype, data),
            Data::J(data) => Series::new(&column.ttype, data),
            Data::K(data) => Series::new(&column.ttype, data),
            Data::A(data) => Series::new(&column.ttype, data),
            Data::E(data) => Series::new(&column.ttype, data),
            Data::D(data) => Series::new(&column.ttype, data),
            Data::C(data) => Series::new(&column.ttype, data),
            Data::M(data) => Series::new(&column.ttype, data),
            Data::P(data) => Series::new(&column.ttype, data),
            Data::Q(data) => Series::new(&column.ttype, data),
        };
        polars_columns.push(series);
    }

    let df = DataFrame::new(polars_columns).map_err(|e| e.to_string())?;
    Ok(df)
}
