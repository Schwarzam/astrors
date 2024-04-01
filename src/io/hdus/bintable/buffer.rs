use num_cpus::get;
use rayon::{prelude::*, vec};
use polars::{prelude::NamedFrom, series::Series};

use crate::io::hdus::bintable::*;



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
    P(Vec<Vec<i32>>), // Array descriptor
    Q(Vec<Vec<i64>>), // Array descriptor
}

#[derive(Debug, PartialEq)]
pub enum ColumnArrayBuffer {
    L(Vec<Vec<bool>>), // Logical
    X(Vec<Vec<u8>>), // Bit
    B(Vec<Vec<i8>>), // Byte
    I(Vec<Vec<i16>>), // Short
    J(Vec<Vec<i32>>), // Int
    K(Vec<Vec<i64>>), // Long
    E(Vec<Vec<f32>>), // Float
    D(Vec<Vec<f64>>), // Double
}

enum BufferTypes
{
    Scalar(ColumnDataBuffer),
    Vector(ColumnArrayBuffer),
}

pub struct Buffer {
    tform : String,
    size : i32,
    buffer : BufferTypes,
    sub_size : i32,
    data_letter : String,
}

impl Buffer {
    pub fn new(tform : &str, size : i32) -> Self {
        let tform = tform.to_string();
        let data_letter = get_first_letter(&tform).to_string();

        let mut sub_size = 1;
        let mut vec_column = false;
        if (get_data_bytes_size(&tform) !=  byte_value_from_str(&tform))
            & (data_letter != "A") & (data_letter != "C") & (data_letter != "M")  & (data_letter != "P") & (data_letter != "Q")
        {
            vec_column = true;
            sub_size = (get_data_bytes_size(&tform)/byte_value_from_str(&tform)) as i32;
        }

        let buffer : BufferTypes;
        if vec_column {
            buffer = BufferTypes::Vector(ColumnArrayBuffer::new(&tform, size, sub_size));
        }else{
            buffer = BufferTypes::Scalar(ColumnDataBuffer::new(&tform, size));
        }

        Buffer{
            tform,
            size,
            buffer,
            sub_size,
            data_letter
        }
    }

    pub fn to_series(&self, col_name : &str) -> Series {
        match &self.buffer {
            BufferTypes::Scalar(data) => data.to_series(col_name),
            BufferTypes::Vector(data) => data.to_series(col_name),
        }
    }

    pub fn clear(&mut self){
        match &mut self.buffer {
            BufferTypes::Scalar(data) => data.clear(),
            BufferTypes::Vector(data) => data.clear(),
        }
    }
    pub fn write_on_idx(&mut self, bytes : &[u8], idx : i64){
        match &mut self.buffer {
            BufferTypes::Scalar(data) => data.write_on_idx(bytes, &self.data_letter, idx),
            BufferTypes::Vector(data) => data.write_on_idx(bytes, &self.data_letter, idx, self.sub_size),
        }
    }


}

impl ColumnArrayBuffer {
    pub fn new(tform : &str, size : i32, sub_size : i32) -> Self {
        let tform = tform.trim();
        let tform_type = get_first_letter(tform);

        match tform_type {
            "L" => ColumnArrayBuffer::L(vec![vec![false; sub_size as usize]; size as usize]),
            "X" => ColumnArrayBuffer::X(vec![vec![0; sub_size as usize]; size as usize]),
            "B" => ColumnArrayBuffer::B(vec![vec![0; sub_size as usize]; size as usize]),
            "I" => ColumnArrayBuffer::I(vec![vec![0; sub_size as usize]; size as usize]),
            "J" => ColumnArrayBuffer::J(vec![vec![0; sub_size as usize]; size as usize]),
            "K" => ColumnArrayBuffer::K(vec![vec![0; sub_size as usize]; size as usize]),
            "E" => ColumnArrayBuffer::E(vec![vec![0.0; sub_size as usize]; size as usize]),
            "D" => ColumnArrayBuffer::D(vec![vec![0.0; sub_size as usize]; size as usize]),
            _   => panic!("Unsupported data type for array col"),
        }
    }

    pub fn to_series(&self, col_name : &str) -> Series {
        let series = match self {
            ColumnArrayBuffer::L(data)   =>  Series::new(col_name, 
                data.iter().map(|vec| {
                    Series::new("", vec)
                }).collect::<Vec<Series>>()
            ),
            ColumnArrayBuffer::X(data)     =>  Series::new(col_name, 
                data.iter().map(|vec| {
                    Series::new("", vec)
                }).collect::<Vec<Series>>()
            ),
            ColumnArrayBuffer::B(data)     =>  Series::new(col_name, 
                data.iter().map(|vec| {
                    Series::new("", vec)
                }).collect::<Vec<Series>>()
            ),
            ColumnArrayBuffer::I(data)    =>  Series::new(col_name, 
                data.iter().map(|vec| {
                    Series::new("", vec)
                }).collect::<Vec<Series>>()
            ),
            ColumnArrayBuffer::J(data)    =>  Series::new(col_name, 
                data.iter().map(|vec| {
                    Series::new("", vec)
                }).collect::<Vec<Series>>()
            ),
            ColumnArrayBuffer::K(data)    =>  Series::new(col_name, 
                data.iter().map(|vec| {
                    Series::new("", vec)
                }).collect::<Vec<Series>>()
            ),
            ColumnArrayBuffer::E(data)    =>  Series::new(col_name, 
                data.iter().map(|vec| {
                    Series::new("", vec)
                }).collect::<Vec<Series>>()
            ),
            ColumnArrayBuffer::D(data)    =>  Series::new(col_name, 
                data.iter().map(|vec| {
                    Series::new("", vec)
                }).collect::<Vec<Series>>()
            ),
        };
        series
    }

    pub fn clear(&mut self){
        match self {
            ColumnArrayBuffer::L(data)   => data.clear(),
            ColumnArrayBuffer::X(data)     => data.clear(),
            ColumnArrayBuffer::B(data)     => data.clear(),
            ColumnArrayBuffer::I(data)    => data.clear(),
            ColumnArrayBuffer::J(data)    => data.clear(),
            ColumnArrayBuffer::K(data)    => data.clear(),
            ColumnArrayBuffer::E(data)    => data.clear(),
            ColumnArrayBuffer::D(data)    => data.clear(),
        }
    }

    pub fn write_on_idx(&mut self, bytes : &[u8], data_letter : &str, idx : i64, sub_size : i32){
        match data_letter {
            "L" => {
                // parse bytes to bool
                match self {
                    ColumnArrayBuffer::L(data) => {
                        (0..sub_size).for_each(|i| {
                            data[idx as usize][i as usize] = bytes[i as usize] != 0;
                        });
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            "K" => {
                // parse bytes to i64
                match self {
                    ColumnArrayBuffer::K(data) => {
                        let mut stbyte = 0;
                        (0..sub_size).for_each(|i| {
                            data[idx as usize][i as usize] = i64::from_be_bytes([bytes[stbyte], bytes[stbyte+1], bytes[stbyte+2], bytes[stbyte+3], bytes[stbyte+4], bytes[stbyte+5], bytes[stbyte+6], bytes[stbyte+7]]);
                            stbyte += 8;
                        });
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            _ => panic!("Wrong data type"),

        }
    }
}

impl ColumnDataBuffer {
    pub fn new(tform : &str, size : i32) -> Self {
        let tform = tform.trim();
        let tform_type = get_first_letter(tform);

        match tform_type {
            "L" => ColumnDataBuffer::L(vec![false; size as usize]),
            "X" => ColumnDataBuffer::X(vec![0; size as usize]),
            "B" => ColumnDataBuffer::B(vec![0; size as usize]),
            "I" => ColumnDataBuffer::I(vec![0; size as usize]),
            "J" => ColumnDataBuffer::J(vec![0; size as usize]),
            "K" => ColumnDataBuffer::K(vec![0; size as usize]),
            "A" => ColumnDataBuffer::A(vec![String::new(); size as usize]),
            "E" => ColumnDataBuffer::E(vec![0.0; size as usize]),
            "D" => ColumnDataBuffer::D(vec![0.0; size as usize]),
            "C" => ColumnDataBuffer::C(vec![String::new(); size as usize]),
            "M" => ColumnDataBuffer::M(vec![String::new(); size as usize]),
            "P" => ColumnDataBuffer::P(vec![vec![0; 2]; size as usize]),
            "Q" => ColumnDataBuffer::Q(vec![vec![0; 2]; size as usize]),
            _   => ColumnDataBuffer::A(vec![String::new(); size as usize]),
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
            ColumnDataBuffer::P(data) =>  {
                let series_vec: Vec<Series> = data.into_iter().map(|vec| {
                    Series::new("", &vec)
                }).collect();
                Series::new(col_name, series_vec)
            },  
            ColumnDataBuffer::Q(data) =>  {
                let series_vec: Vec<Series> = data.into_iter().map(|vec| {
                    Series::new("", &vec)
                }).collect();
                Series::new(col_name, series_vec)
            }
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

    pub fn write_on_idx(&mut self, bytes : &[u8], data_letter : &str, idx : i64){
        match data_letter {
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
                        data[idx as usize][0] = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                        data[idx as usize][1] = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            "Q" => {
                // parse bytes to String
                match self {
                    ColumnDataBuffer::Q(data) => {
                        data[idx as usize][0] = i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]);
                        data[idx as usize][0] = i64::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]]);
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            _ => panic!("Wrong data type"),

        }
    }
    //no need for max_len on bintable
}