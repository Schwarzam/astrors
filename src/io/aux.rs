use rayon::prelude::*;
use ndarray::{ArrayD, IxDyn};

pub enum DataType {
    Int8 = 8,
    Int16 = 16,
    Int32 = 32,
    Float32 = -32,
    Float64 = -64
}

impl DataType {
    pub fn nbytes(&self) -> usize {
        match self {
            DataType::Int8 => 1,    // 8 bits = 1 byte
            DataType::Int16 => 2,   // 16 bits = 2 bytes
            DataType::Int32 => 4,   // 32 bits = 4 bytes
            DataType::Float32 => 4, // 32 bits = 4 bytes
            DataType::Float64 => 8, // 64 bits = 8 bytes
        }
    }

    pub fn from_bitpix(bitpix: i32) -> Option<DataType> {
        match bitpix {
            8 => Some(DataType::Int8),
            16 => Some(DataType::Int16),
            32 => Some(DataType::Int32),
            -32 => Some(DataType::Float32),
            -64 => Some(DataType::Float64),
            _ => panic!("Unknown bitpix value"),
        }
    }
}

pub fn bytes_to_i8_vec(bytes: &[u8]) -> Vec<i8> {
    bytes.iter().map(|&x| x as i8).collect()
}

pub fn bytes_to_i16_vec(bytes: &[u8]) -> Vec<i16> {
    bytes
        .par_chunks(2)
        .map(|b| i16::from_be_bytes([b[0], b[1]]))
        .collect()
}

pub fn bytes_to_i32_vec(bytes: &[u8]) -> Vec<i32> {
    bytes
        .par_chunks(4)
        .map(|b| i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
        .collect()
}

pub fn bytes_to_f32_vec(bytes: &[u8]) -> Vec<f32> {
    bytes
        .par_chunks(4)
        .map(|b| f32::from_bits(u32::from_be_bytes([b[0], b[1], b[2], b[3]])))
        .collect()
}

pub fn bytes_to_f64_vec(bytes: &[u8]) -> Vec<f64> {
    bytes
        .par_chunks(8)
        .map(|b| f64::from_bits(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])))
        .collect()
}

pub fn vec_to_ndarray<T>(data: Vec<T>, shape: Vec<usize>) -> ArrayD<T> {
    let shape_ix = IxDyn(&shape);
    ArrayD::from_shape_vec(shape_ix, data).unwrap() // handle the error appropriately in your code
}