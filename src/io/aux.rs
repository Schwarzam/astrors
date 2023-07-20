use ndarray::{ArrayD, IxDyn};

pub enum DataType {
    Int8 = 8,
    Int16 = 16,
    Int32 = 32,
    Int64 = 64,
    Float32 = -32,
    Float64 = -64
}

impl DataType {
    pub fn nbytes(&self) -> usize {
        match self {
            DataType::Int8 => 1,    // 8 bits = 1 byte
            DataType::Int16 => 2,   // 16 bits = 2 bytes
            DataType::Int32 => 4,   // 32 bits = 4 bytes
            DataType::Int64 => 8,   // 64 bits = 8 bytes
            DataType::Float32 => 4, // 32 bits = 4 bytes
            DataType::Float64 => 8, // 64 bits = 8 bytes
        }
    }

    pub fn from_bitpix(bitpix: i32) -> Option<DataType> {
        match bitpix {
            8 => Some(DataType::Int8),
            16 => Some(DataType::Int16),
            32 => Some(DataType::Int32),
            64 => Some(DataType::Int64),
            -32 => Some(DataType::Float32),
            -64 => Some(DataType::Float64),
            _ => panic!("Unknown bitpix value"),
        }
    }
}

pub fn bytes_to_f32_vec(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks(4)
        .map(|b| f32::from_bits(u32::from_be_bytes([b[0], b[1], b[2], b[3]])))
        .collect()
}

pub fn vec_to_ndarray(data: Vec<f32>, shape: Vec<usize>) -> ArrayD<f32> {
    let shape_ix = IxDyn(&shape);
    ArrayD::from_shape_vec(shape_ix, data).unwrap() // handle the error appropriately in your code
}