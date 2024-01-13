use core::panic;
use std::{fs::File, io::{Read, Write}};

use crate::io::{Header, header::card::Card, utils::pad_buffer_to_fits_block};
use polars::prelude::*; // Polars library
use crate::io::hdus::table::table_utils::*;

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

    pub fn max_len(&self) -> usize {
        match self {
            Data::L(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::X(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::B(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::I(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::J(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::K(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::A(data) => data.iter().map(|x| x.len()).max().unwrap_or(0),
            Data::E(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::D(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::C(data) => data.iter().map(|x| x.len()).max().unwrap_or(0),
            Data::M(data) => data.iter().map(|x| x.len()).max().unwrap_or(0),
            Data::P(data) => data.iter().map(|x| x.len()).max().unwrap_or(0),
            Data::Q(data) => data.iter().map(|x| x.len()).max().unwrap_or(0),
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

pub fn polars_to_columns(df: DataFrame) -> Result<Vec<Column>, std::io::Error> {
    let mut columns: Vec<Column> = Vec::new();
    
    for series in df.get_columns() {
        let data = match series.dtype() {
            DataType::Boolean => {
                let data = series_to_vec_bool(series).unwrap();
                Data::L(data)
            },
            DataType::UInt8 => {
                let data = series_to_vec_u8(series).unwrap();
                Data::X(data)
            },
            DataType::Int8 => {
                let data = series_to_vec_i8(series).unwrap();
                Data::B(data)
            },
            DataType::Int16 => {
                let data = series_to_vec_i16(series).unwrap();
                Data::I(data)
            },
            DataType::Int32 => {
                let data = series_to_vec_i32(series).unwrap();
                Data::J(data)
            },
            DataType::Int64 => {
                let data = series_to_vec_i64(series).unwrap();
                Data::K(data)
            },
            DataType::Float32 => {
                let data = series_to_vec_f32(series).unwrap();
                Data::E(data)
            },
            DataType::Float64 => {
                let data = series_to_vec_f64(series).unwrap();
                Data::D(data)
            },
            DataType::String => {
                let data = series_to_vec_string(series).unwrap();
                Data::A(data)
            },
            _ => {
                let data = series_to_vec_string(series).unwrap();
                Data::A(data)
            }
        };
        
        let column = Column {
            ttype: series.name().to_string(),
            tform: "1A".to_string(),
            tunit: None,
            tdisp: None,
            data,
        };
        columns.push(column);
    }

    for column in columns.iter_mut() {
        let formatted_string;
        let tform = match &column.data {
            Data::L(_) => "L",
            Data::X(_) => "X",
            Data::B(_) => "B",
            Data::I(_) => "I",
            Data::J(_) => "J",
            Data::K(_) => "K",
            Data::A(data) => {
                formatted_string = format!("{}A", column.data.max_len());
                //formatted_string = format!("A48");
                &formatted_string
            },
            Data::E(_) => "E",
            Data::D(_) => "D",
            Data::C(_) => "C",
            Data::M(_) => "M",
            Data::P(_) => "P",
            Data::Q(_) => "Q",
        };
        column.tform = tform.to_string();
        
        let (_, size) = get_tform_type_size(&column.tform);
    }


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
    header.add_card(&Card::new("NAXIS2".to_string(), columns[0].data.len().to_string(), Some("Number of rows".to_string())));
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
    let rows = columns[0].data.len();
    let mut bytes_written = 0;
    for row in 0..rows {
        for column in columns.iter() {
            let (_, size) = get_tform_type_size(&column.tform);

            let buffer = match &column.data {
                Data::L(data) => data[row].to_string().into_bytes(),
                Data::X(data) => data[row].to_string().into_bytes(),
                Data::B(data) => data[row].to_be_bytes().to_vec(),
                Data::I(data) => data[row].to_be_bytes().to_vec(),
                Data::J(data) => data[row].to_be_bytes().to_vec(),
                Data::K(data) => data[row].to_be_bytes().to_vec(),
                Data::A(data) => {
                    let mut string = data[row].clone();
                    string.push_str(&" ".repeat(column.data.max_len() - data[row].len()));
                    string.into_bytes()
                },
                Data::E(data) => data[row].to_be_bytes().to_vec(),
                Data::D(data) => data[row].to_be_bytes().to_vec(),
                Data::C(data) => data[row].clone().into_bytes(),
                Data::M(data) => data[row].clone().into_bytes(),
                Data::P(data) => data[row].clone().into_bytes(),
                Data::Q(data) => data[row].clone().into_bytes(),
            };
            
            bytes_written += buffer.len();
            // println!("Buffer: {:?}", String::from_utf8_lossy(&buffer));
            file.write_all(&buffer)?;
        }
    }
    pad_buffer_to_fits_block(file, bytes_written)?;
    Ok(())
}