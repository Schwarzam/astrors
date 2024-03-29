use rayon::prelude::*;
use polars::{prelude::NamedFrom, series::Series};

#[derive(Debug, PartialEq)]
pub enum ColumnDataBuffer {
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

impl ColumnDataBuffer {
    pub fn new(tform : &str, size : i32) -> Self {
        let tform = tform.trim();
        let tform_type = tform.chars().last().unwrap_or('A');
        
        match tform_type {
            'L' => ColumnDataBuffer::L(vec![false; size as usize]),
            'X' => ColumnDataBuffer::X(vec![0; size as usize]),
            'B' => ColumnDataBuffer::B(vec![0; size as usize]),
            'I' => ColumnDataBuffer::I(vec![0; size as usize]),
            'J' => ColumnDataBuffer::J(vec![0; size as usize]),
            'K' => ColumnDataBuffer::K(vec![0; size as usize]),
            'A' => ColumnDataBuffer::A(vec![String::new(); size as usize]),
            'E' => ColumnDataBuffer::E(vec![0.0; size as usize]),
            'D' => ColumnDataBuffer::D(vec![0.0; size as usize]),
            'C' => ColumnDataBuffer::C(vec![String::new(); size as usize]),
            'M' => ColumnDataBuffer::M(vec![String::new(); size as usize]),
            'P' => ColumnDataBuffer::P(vec![String::new(); size as usize]),
            'Q' => ColumnDataBuffer::Q(vec![String::new(); size as usize]),
            _   => ColumnDataBuffer::A(vec![String::new(); size as usize]),
        }
    }

    pub fn max_len(&self) -> usize {
        match self {
            ColumnDataBuffer::L(data)   => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::X(data)     => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::B(data)     => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::I(data)    => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::J(data)    => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::K(data)    => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::A(data) => data.par_iter().map(|x| x.len()).max().unwrap_or(0),
            ColumnDataBuffer::E(data)    => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::D(data)    => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::C(data) => data.par_iter().map(|x| x.len()).max().unwrap_or(0),
            ColumnDataBuffer::M(data) => data.par_iter().map(|x| x.len()).max().unwrap_or(0),
            ColumnDataBuffer::P(data) => data.par_iter().map(|x| x.len()).max().unwrap_or(0),
            ColumnDataBuffer::Q(data) => data.par_iter().map(|x| x.len()).max().unwrap_or(0),
        }
    }

    pub fn byte_value(&self) -> usize{
        match self {
            ColumnDataBuffer::L(_) => 1,
            ColumnDataBuffer::X(_) => 1,
            ColumnDataBuffer::B(_) => 1,
            ColumnDataBuffer::I(_) => 2,
            ColumnDataBuffer::J(_) => 4,
            ColumnDataBuffer::K(_) => 8,
            ColumnDataBuffer::A(_) => 1,
            ColumnDataBuffer::E(_) => 4,
            ColumnDataBuffer::D(_) => 8,
            ColumnDataBuffer::C(_) => 8,
            ColumnDataBuffer::M(_) => 16,
            ColumnDataBuffer::P(_) => 8,
            ColumnDataBuffer::Q(_) => 16,
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

    pub fn to_series(&self, col_name : &str) -> Series {
        let series = match self {
            ColumnDataBuffer::L(data)   =>  Series::new(col_name, data),
            ColumnDataBuffer::X(data)     =>  Series::new(col_name, data),
            ColumnDataBuffer::B(data)     =>  Series::new(col_name, data),
            ColumnDataBuffer::I(data)    =>  Series::new(col_name, data),
            ColumnDataBuffer::J(data)    =>  Series::new(col_name, data),
            ColumnDataBuffer::K(data)    =>  Series::new(col_name, data),
            ColumnDataBuffer::A(data) =>  Series::new(col_name, data),
            ColumnDataBuffer::E(data)    =>  Series::new(col_name, data),
            ColumnDataBuffer::D(data)    =>  Series::new(col_name, data),
            ColumnDataBuffer::C(data) =>  Series::new(col_name, data),
            ColumnDataBuffer::M(data) =>  Series::new(col_name, data),
            ColumnDataBuffer::P(data) =>  Series::new(col_name, data),  
            ColumnDataBuffer::Q(data) =>  Series::new(col_name, data)
        };
        series
    }

    pub fn clear(&mut self){
        match self {
            ColumnDataBuffer::L(data)   => data.clear(),
            ColumnDataBuffer::X(data)     => data.clear(),
            ColumnDataBuffer::B(data)     => data.clear(),
            ColumnDataBuffer::I(data)    => data.clear(),
            ColumnDataBuffer::J(data)    => data.clear(),
            ColumnDataBuffer::K(data)    => data.clear(),
            ColumnDataBuffer::A(data) => data.clear(),
            ColumnDataBuffer::E(data)    => data.clear(),
            ColumnDataBuffer::D(data)    => data.clear(),
            ColumnDataBuffer::C(data) => data.clear(),
            ColumnDataBuffer::M(data) => data.clear(),
            ColumnDataBuffer::P(data) => data.clear(),
            ColumnDataBuffer::Q(data) => data.clear(),
        }
    }

    pub fn write_on_idx(&mut self, bytes : &[u8], data_type : &str, idx : i64){
        match data_type {
            "L" => {
                // parse bytes to bool
                match self {
                    ColumnDataBuffer::L(data) => data[idx as usize] = bytes[0] != 0,
                    _ => panic!("Wrong data type"),
                }
            }
            "X" => {
                // parse bytes to u8
                match self {
                    ColumnDataBuffer::X(data) => data[idx as usize] = bytes[0],
                    _ => panic!("Wrong data type"),
                }
            }
            "B" => {
                // parse bytes to i8
                match self {
                    ColumnDataBuffer::B(data) => data[idx as usize] = bytes[0] as i8,
                    _ => panic!("Wrong data type"),
                }
            }
            "I" => {
                // parse bytes to i16
                match self {
                    ColumnDataBuffer::I(data) => data[idx as usize] = i16::from_be_bytes([bytes[0], bytes[1]]),
                    _ => panic!("Wrong data type"),
                }
            }
            "J" => {
                // parse bytes to i32
                match self {
                    ColumnDataBuffer::J(data) => data[idx as usize] = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                    _ => panic!("Wrong data type"),
                }
            }
            "K" => {
                // parse bytes to i64
                match self {
                    ColumnDataBuffer::K(data) => data[idx as usize] = i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]),
                    _ => panic!("Wrong data type"),
                }
            }
            "A" => {
                // parse bytes to String
                match self {
                    ColumnDataBuffer::A(data) => {
                        let string = unsafe { String::from_utf8_unchecked(bytes.to_vec()) }.trim_end().to_string();
                        data[idx as usize] = string;
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            "E" => {
                // parse bytes to f32
                match self {
                    ColumnDataBuffer::E(data) => data[idx as usize] = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                    _ => panic!("Wrong data type"),
                }
            }
            "D" => {
                // parse bytes to f64
                match self {
                    ColumnDataBuffer::D(data) => data[idx as usize] = f64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]),
                    _ => panic!("Wrong data type"),
                }
            }
            "C" => {
                // parse bytes to String
                match self {
                    ColumnDataBuffer::C(data) => {
                        let string = unsafe { String::from_utf8_unchecked(bytes.to_vec()) }.trim_end().to_string();
                        data[idx as usize] = string;
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            "M" => {
                // parse bytes to String
                match self {
                    ColumnDataBuffer::M(data) => {
                        let string = unsafe { String::from_utf8_unchecked(bytes.to_vec()) }.trim_end().to_string();
                        data[idx as usize] = string;
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            "P" => {
                // parse bytes to String
                match self {
                    ColumnDataBuffer::P(data) => {
                        let string = unsafe { String::from_utf8_unchecked(bytes.to_vec()) }.trim_end().to_string();
                        data[idx as usize] = string;
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            "Q" => {
                // parse bytes to String
                match self {
                    ColumnDataBuffer::Q(data) => {
                        let string = unsafe { String::from_utf8_unchecked(bytes.to_vec()) }.trim_end().to_string();
                        data[idx as usize] = string;
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            _ => panic!("Wrong data type"),

        }
    }
    //no need for max_len on bintable
}