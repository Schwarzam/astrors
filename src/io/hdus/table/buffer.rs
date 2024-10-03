use polars::{datatypes::PlSmallStr, prelude::NamedFrom, series::Series};
use rayon::prelude::*;

#[derive(Debug, PartialEq)]
pub enum ColumnDataBuffer {
    I(Vec<i32>),
    E(Vec<f32>),
    D(Vec<f64>),
    A(Vec<String>),
    F(Vec<f32>),
}

impl ColumnDataBuffer {
    pub fn new(tform: &str, size: i32) -> Self {
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

    pub fn max_len(&self) -> usize {
        match self {
            ColumnDataBuffer::I(data) => data
                .par_iter()
                .map(|x| x.to_string().len())
                .max()
                .unwrap_or(0),
            ColumnDataBuffer::E(data) => data
                .par_iter()
                .map(|x| x.to_string().len())
                .max()
                .unwrap_or(0),
            ColumnDataBuffer::D(data) => data
                .par_iter()
                .map(|x| x.to_string().len())
                .max()
                .unwrap_or(0),
            ColumnDataBuffer::A(data) => data.par_iter().map(|x| x.len()).max().unwrap_or(0),
            ColumnDataBuffer::F(data) => data
                .par_iter()
                .map(|x| x.to_string().len())
                .max()
                .unwrap_or(0),
        }
    }

    pub fn byte_value(&self) -> usize {
        match self {
            ColumnDataBuffer::I(_data) => 4,
            ColumnDataBuffer::E(_data) => 4,
            ColumnDataBuffer::D(_data) => 8,
            ColumnDataBuffer::A(_data) => 1,
            ColumnDataBuffer::F(_data) => 4,
        }
    }

    pub fn byte_value_from_str(data_type: &str) -> usize {
        match data_type {
            "I" => 4,
            "E" => 4,
            "D" => 8,
            "A" => 1,
            "F" => 4,
            _ => panic!("Wrong data type"),
        }
    }

    pub fn to_series(&self, col_name: &str) -> Series {
        let col_name = PlSmallStr::from_str(col_name);
        let series = match self {
            ColumnDataBuffer::I(data) => Series::new(col_name, data),
            ColumnDataBuffer::E(data) => Series::new(col_name, data),
            ColumnDataBuffer::D(data) => Series::new(col_name, data),
            ColumnDataBuffer::A(data) => Series::new(col_name, data),
            ColumnDataBuffer::F(data) => Series::new(col_name, data),
        };
        series
    }

    pub fn clear(&mut self) {
        match self {
            ColumnDataBuffer::I(data) => data.clear(),
            ColumnDataBuffer::E(data) => data.clear(),
            ColumnDataBuffer::D(data) => data.clear(),
            ColumnDataBuffer::A(data) => data.clear(),
            ColumnDataBuffer::F(data) => data.clear(),
        }
    }

    pub fn write_on_idx(&mut self, bytes: &[u8], data_type: char, idx: i64) {
        let string = String::from_utf8_lossy(&bytes)
            .trim_end()
            .trim_start()
            .to_string();
        match data_type {
            'I' => {
                // parse bytes to i32
                match self {
                    ColumnDataBuffer::I(data) => {
                        data[idx as usize] = string.parse::<i32>().unwrap()
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            'E' => {
                // parse bytes to f32
                match self {
                    ColumnDataBuffer::E(data) => {
                        data[idx as usize] = string.parse::<f32>().unwrap()
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            'D' => {
                // parse bytes to f64
                match self {
                    ColumnDataBuffer::D(data) => {
                        data[idx as usize] = string.parse::<f64>().unwrap()
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            'A' => {
                // parse bytes to String
                match self {
                    ColumnDataBuffer::A(data) => data[idx as usize] = string,
                    _ => panic!("Wrong data type"),
                }
            }
            'F' => {
                // parse bytes to f32
                match self {
                    ColumnDataBuffer::F(data) => {
                        data[idx as usize] = string.parse::<f32>().unwrap()
                    }
                    _ => panic!("Wrong data type"),
                }
            }
            _ => panic!("Wrong data type"),
        }
    }
    //no need for max_len on bintable
}
