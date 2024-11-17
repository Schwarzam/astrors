use num_cpus::get;
use rayon::{prelude::*, vec};
use polars::{prelude::NamedFrom, series::Series};

use crate::io::hdus::bintable::*;


/// Represents a data buffer for a column in a FITS binary table.
///
/// # Variants
/// - `L(Vec<bool>)`: Logical data (boolean values).
/// - `X(Vec<u8>)`: Bit data.
/// - `B(Vec<i8>)`: Byte data (8-bit signed integers).
/// - `I(Vec<i16>)`: Short data (16-bit signed integers).
/// - `J(Vec<i32>)`: Integer data (32-bit signed integers).
/// - `K(Vec<i64>)`: Long data (64-bit signed integers).
/// - `A(Vec<String>)`: Character data (ASCII strings).
/// - `E(Vec<f32>)`: Float data (32-bit floating-point values).
/// - `D(Vec<f64>)`: Double data (64-bit floating-point values).
/// - `C(Vec<String>)`: Complex data (stored as strings for simplicity).
/// - `M(Vec<String>)`: Double complex data (stored as strings for simplicity).
/// - `P(Vec<Vec<i32>>)` and `Q(Vec<Vec<i64>>)`:
///   - Array descriptors for variable-length columns.
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

/// Represents an array buffer for columns with vector data in a FITS binary table.
///
/// # Variants
/// - `L(Vec<Vec<bool>>)`
/// - `X(Vec<Vec<u8>>)`
/// - `B(Vec<Vec<i8>>)`
/// - `I(Vec<Vec<i16>>)`
/// - `J(Vec<Vec<i32>>)`
/// - `K(Vec<Vec<i64>>)`
/// - `E(Vec<Vec<f32>>)`
/// - `D(Vec<Vec<f64>>)`
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

/// Represents a generalized buffer for a FITS binary table column.
///
/// # Fields
/// - `tform` (String): The format string of the column (e.g., "J", "D").
/// - `size` (i32): The number of rows in the column.
/// - `buffer` (BufferTypes): The actual data buffer (scalar or vector).
/// - `sub_size` (i32): The size of sub-elements in case of vector columns.
/// - `data_letter` (String): The first letter of the `TFORM` format string, indicating the data type.
pub struct Buffer {
    tform : String,
    size : i32,
    buffer : BufferTypes,
    sub_size : i32,
    data_letter : String,
}

impl Buffer {
    /// Constructs a new `Buffer` for a FITS binary table column.
    ///
    /// # Arguments
    /// - `tform` (&str): The format string for the column (e.g., "J", "D").
    /// - `size` (i32): The number of rows in the column.
    ///
    /// # Returns
    /// - `Buffer`: A new buffer initialized based on the column's format and size.
    ///
    /// # Behavior
    /// - Initializes the buffer as either scalar or vector, depending on the format string.
    /// - Calculates the `sub_size` for vector columns.
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

    /// Converts the buffer into a Polars `Series` for data analysis.
    ///
    /// # Arguments
    /// - `col_name` (&str): The name of the column.
    ///
    /// # Returns
    /// - `Series`: A Polars `Series` containing the column's data.
    pub fn to_series(&self, col_name : &str) -> Series {
        match &self.buffer {
            BufferTypes::Scalar(data) => data.to_series(col_name),
            BufferTypes::Vector(data) => data.to_series(col_name),
        }
    }

    /// Clears the buffer, removing all stored data.
    pub fn clear(&mut self){
        match &mut self.buffer {
            BufferTypes::Scalar(data) => data.clear(),
            BufferTypes::Vector(data) => data.clear(),
        }
    }

    /// Writes data to the buffer at a specified row index.
    ///
    /// # Arguments
    /// - `bytes` (&[u8]): The raw bytes to write.
    /// - `idx` (i64): The row index where the data should be written.
    pub fn write_on_idx(&mut self, bytes : &[u8], idx : i64){
        match &mut self.buffer {
            BufferTypes::Scalar(data) => data.write_on_idx(bytes, &self.data_letter, idx),
            BufferTypes::Vector(data) => data.write_on_idx(bytes, &self.data_letter, idx, self.sub_size),
        }
    }

    /// Reads variable-length column data (e.g., `P` and `Q` type columns).
    ///
    /// # Behavior
    /// - Processes the buffer to extract variable-length data, if applicable.
    pub fn read_var_len_cols(&mut self){
        //TODO
        
        match &mut self.buffer {
            BufferTypes::Scalar(data) => {
                println!("buffer: {:?}", data);
            },
            BufferTypes::Vector(data) => {
                
            },
        }

    }


}

impl ColumnArrayBuffer {
    /// Constructs a new `ColumnArrayBuffer` for array data in a column.
    ///
    /// # Arguments
    /// - `tform` (&str): The format string for the column.
    /// - `size` (i32): The number of rows in the column.
    /// - `sub_size` (i32): The size of sub-elements in the array.
    ///
    /// # Returns
    /// - `ColumnArrayBuffer`: A new buffer for storing array data.
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

    pub fn empty(tform : &str) -> Self {
        let tform = tform.trim();
        let tform_type = get_first_letter(tform);

        match tform_type {
            "L" => ColumnArrayBuffer::L(vec![vec![]]),
            "X" => ColumnArrayBuffer::X(vec![vec![]]),
            "B" => ColumnArrayBuffer::B(vec![vec![]]),
            "I" => ColumnArrayBuffer::I(vec![vec![]]),
            "J" => ColumnArrayBuffer::J(vec![vec![]]),
            "K" => ColumnArrayBuffer::K(vec![vec![]]),
            "E" => ColumnArrayBuffer::E(vec![vec![]]),
            "D" => ColumnArrayBuffer::D(vec![vec![]]),
            _   => panic!("Unsupported data type for array col"),
        }
    }

    /// Converts the array buffer into a Polars `Series`.
    ///
    /// # Arguments
    /// - `col_name` (&str): The name of the column.
    ///
    /// # Returns
    /// - `Series`: A Polars `Series` containing the column's array data.
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

    /// Clears the array buffer, removing all stored data.
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

    /// Writes data to the array buffer at a specified row index.
    ///
    /// # Arguments
    /// - `bytes` (&[u8]): The raw bytes to write.
    /// - `data_letter` (&str): The format letter indicating the data type.
    /// - `idx` (i64): The row index where the data should be written.
    /// - `sub_size` (i32): The number of sub-elements for vector data.
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
    /// Constructs a new `ColumnDataBuffer` for scalar data in a column.
    ///
    /// # Arguments
    /// - `tform` (&str): The format string for the column.
    /// - `size` (i32): The number of rows in the column.
    ///
    /// # Returns
    /// - `ColumnDataBuffer`: A new buffer for storing scalar data.
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

    /// Converts the scalar data buffer into a Polars `Series`.
    ///
    /// # Arguments
    /// - `col_name` (&str): The name of the column.
    ///
    /// # Returns
    /// - `Series`: A Polars `Series` containing the column's scalar data.
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

    /// Clears the scalar data buffer, removing all stored data.
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

    /// Writes data to the scalar buffer at a specified row index.
    ///
    /// # Arguments
    /// - `bytes` (&[u8]): The raw bytes to write.
    /// - `data_letter` (&str): The format letter indicating the data type.
    /// - `idx` (i64): The row index where the data should be written.
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