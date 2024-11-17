use rayon::prelude::*;
use polars::{prelude::NamedFrom, series::Series};

/// Enum representing column data types in a binary table.
/// Each variant corresponds to a specific data type.
#[derive(Debug, PartialEq)]
pub enum ColumnDataBuffer {
    I(Vec<i32>),
    E(Vec<f32>),
    D(Vec<f64>),
    A(Vec<String>),
    F(Vec<f32>),
}

impl ColumnDataBuffer {
    /// Creates a new `ColumnDataBuffer` based on the FITS column format (TFORM) and size.
    ///
    /// # Arguments
    /// * `tform` - A string specifying the FITS column format.
    /// * `size` - The number of elements to allocate in the buffer.
    ///
    /// # Panics
    /// Panics if the `tform` specifies an unsupported data type.
    pub fn new(tform : &str, size : i32) -> Self {
        let tform = tform.trim();
        let tform_type = tform.chars().next().unwrap();
        match tform_type {
            'I' => ColumnDataBuffer::I(vec![0; size as usize]),
            'E' => ColumnDataBuffer::E(vec![0.0; size as usize]),
            'D' => ColumnDataBuffer::D(vec![0.0; size as usize]),
            'A' => ColumnDataBuffer::A(vec![String::new(); size as usize]),
            'F' => ColumnDataBuffer::F(vec![0.0; size as usize]),
            _ => panic!("Wrong data type"),
        }
    }

    /// Computes the maximum length of elements in the buffer.
    ///
    /// # Returns
    /// The maximum length of elements when converted to strings (for numeric types)
    /// or the maximum string length (for string type).
    pub fn max_len(&self) -> usize {
        match self {
            ColumnDataBuffer::I(data) => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::E(data) => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::D(data) => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            ColumnDataBuffer::A(data) => data.par_iter().map(|x| x.len()).max().unwrap_or(0),
            ColumnDataBuffer::F(data) => data.par_iter().map(|x| x.to_string().len()).max().unwrap_or(0),
        }
    }

    /// Returns the byte size of the elements in the buffer.
    ///
    /// # Returns
    /// The size in bytes of a single element in the buffer.
    pub fn byte_value(&self) -> usize{
        match self {
            ColumnDataBuffer::I(_data) => 4,
            ColumnDataBuffer::E(_data) => 4,
            ColumnDataBuffer::D(_data) => 8,
            ColumnDataBuffer::A(_data) => 1,
            ColumnDataBuffer::F(_data) => 4,
        }
    }

    /// Returns the byte size of the specified data type.
    ///
    /// # Arguments
    /// * `data_type` - A string representing the FITS data type.
    ///
    /// # Returns
    /// The size in bytes of the specified data type.
    ///
    /// # Panics
    /// Panics if the `data_type` is invalid.
    pub fn byte_value_from_str(data_type : &str) -> usize {
        match data_type {
            "I" => 4,
            "E" => 4,
            "D" => 8,
            "A" => 1,
            "F" => 4,
            _ => panic!("Wrong data type"),
        }
    }

    /// Converts the buffer into a Polars `Series` for analysis or manipulation.
    ///
    /// # Arguments
    /// * `col_name` - The name of the column in the resulting `Series`.
    ///
    /// # Returns
    /// A `Series` representing the data in the buffer.
    pub fn to_series(&self, col_name : &str) -> Series {
        
        match self {
            ColumnDataBuffer::I(data) =>  Series::new(col_name, data),
            ColumnDataBuffer::E(data) =>  Series::new(col_name, data),
            ColumnDataBuffer::D(data) =>  Series::new(col_name, data),
            ColumnDataBuffer::A(data) =>  Series::new(col_name, data),
            ColumnDataBuffer::F(data) =>  Series::new(col_name, data),
        }
    }

    /// Clears all elements in the buffer.
    pub fn clear(&mut self){
        match self {
            ColumnDataBuffer::I(data) => data.clear(),
            ColumnDataBuffer::E(data) => data.clear(),
            ColumnDataBuffer::D(data) => data.clear(),
            ColumnDataBuffer::A(data) => data.clear(),
            ColumnDataBuffer::F(data) => data.clear(),
        }
    }

    /// Writes data into the buffer at a specified index.
    ///
    /// # Arguments
    /// * `bytes` - A slice of bytes representing the value to write.
    /// * `data_type` - The data type of the value (`I`, `E`, `D`, `A`, `F`).
    /// * `idx` - The index at which to write the value.
    ///
    /// # Panics
    /// Panics if the data type does not match the buffer type or if parsing fails.
    pub fn write_on_idx(&mut self, bytes : &[u8], data_type : char, idx : i64){
        let string = String::from_utf8_lossy(bytes).trim_end().trim_start().to_string();
        match data_type {
            'I' => {
                // parse bytes to i32
                match self {
                    ColumnDataBuffer::I(data) => data[idx as usize] = string.parse::<i32>().unwrap(),
                    _ => panic!("Wrong data type"),
                }
            },
            'E' => {
                // parse bytes to f32
                match self {
                    ColumnDataBuffer::E(data) => data[idx as usize] = string.parse::<f32>().unwrap(),
                    _ => panic!("Wrong data type"),
                }
            },
            'D' => {
                // parse bytes to f64
                match self {
                    ColumnDataBuffer::D(data) => data[idx as usize] = string.parse::<f64>().unwrap(),
                    _ => panic!("Wrong data type"),
                }
            },
            'A' => {
                // parse bytes to String
                match self {
                    ColumnDataBuffer::A(data) => data[idx as usize] = string,
                    _ => panic!("Wrong data type"),
                }
            },
            'F' => {
                // parse bytes to f32
                match self {
                    ColumnDataBuffer::F(data) => data[idx as usize] = string.parse::<f32>().unwrap(),
                    _ => panic!("Wrong data type"),
                }
            },
            _ => panic!("Wrong data type"),
        }
    }
    //no need for max_len on bintable
}