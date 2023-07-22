use std::fs::File;
use std::io::Read;
use crate::io::header::Header;

use memmap::Mmap;
use rayon::vec;


use crate::io::aux::{
    get_shape,
    bytes_to_f64_vec,
    pre_bytes_to_f64_vec,
    pre_bytes_to_f32_vec,
    pre_bytes_to_i8_vec,
    pre_bytes_to_i16_vec,
    pre_bytes_to_i32_vec, 
    vec_to_ndarray, 
    DataType
};
use ndarray::{ArrayD, IxDyn};

 

pub struct Data {
    data: ndarray::ArrayD<DataType>,
}

struct DataArray {
    int8: Option<Vec<i8>>,
    int16: Option<Vec<i16>>,
    int32: Option<Vec<i32>>,
    float32: Option<Vec<f32>>,
    float64: Option<Vec<f64>>,
}

impl DataArray {
    fn new() -> Self{
        DataArray {
            int8: None,
            int16: None,
            int32: None,
            float32: None,
            float64: None,
        }
    }
    fn <T>get_active() -> Vec<T>{
        let mut active = Vec::new();
        if self.int8.is_some() {
            active.push(self.int8.unwrap());
        }
        if self.int16.is_some() {
            active.push(self.int16.unwrap());
        }
        if self.int32.is_some() {
            active.push(self.int32.unwrap());
        }
        if self.float32.is_some() {
            active.push(self.float32.unwrap());
        }
        if self.float64.is_some() {
            active.push(self.float64.unwrap());
        }
        active
    }

}

impl Data {

    pub fn read_from_filebytes(f: &mut File, header: &mut Header) -> Result<(), std::io::Error>  {
        use std::time::Instant;
        let now = Instant::now();
    
        let naxis: usize = header.parse_header_value("NAXIS")?;
        let naxis1: usize = header.parse_header_value("NAXIS1")?;
        let naxis2: usize = header.parse_header_value("NAXIS2")?;
    
        let bitpix : i32 = header.parse_header_value("BITPIX")?;
    
        // Get data type from BITPIX
        let dtype = DataType::from_bitpix(bitpix).unwrap();
        let nbytes = dtype.nbytes();
        // println!("nbytes: {:?}", nbytes);
    
        let mut databuf = vec![0; naxis1 * naxis2 * nbytes];
    
        let data = f.read(&mut databuf)?;
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
    
        let mut data_array : DataArray = DataArray::new();
        match dtype { //Pre allocate
            DataType::Int8 => {
                data_array.int8 = Some( vec![0i8; naxis1 * naxis2] );
                pre_bytes_to_i8_vec(databuf, &mut data_array.int8.unwrap());
            },
            DataType::Int16 => {
                data_array.int16 = Some( vec![0i16; naxis1 * naxis2] );
                pre_bytes_to_i16_vec(databuf, &mut data_array.int16.unwrap());
            },
            DataType::Int32 => {
                data_array.int32 = Some( vec![0i32; naxis1 * naxis2] );
                pre_bytes_to_i32_vec(databuf, &mut data_array.int32.unwrap());
            },
            DataType::Float32 => {
                data_array.float32 = Some( vec![0.0f32; naxis1 * naxis2] );
                pre_bytes_to_f32_vec(databuf, &mut data_array.float32.unwrap());
            },
            DataType::Float64 => {
                data_array.float64 = Some( vec![0.0f64; naxis1 * naxis2] );
                pre_bytes_to_f64_vec(databuf, &mut data_array.float64.unwrap());
            },
        }
    
        let shape = get_shape(header)?;
    
        let ndarray = match dtype {
            DataType::Int8 => {
                vec_to_ndarray::<i8>(data_array.int8.unwrap(), shape)
            },
            DataType::Int16 => {
                vec_to_ndarray::<i16>(data_array.int16.unwrap(), shape)
            },
            DataType::Int32 => {
                vec_to_ndarray::<i32>(data_array.int32.unwrap(), shape)
            },
            DataType::Float32 => {
                vec_to_ndarray::<f32>(data_array.float32.unwrap(), shape)
            },
            DataType::Float64 => {
                vec_to_ndarray::<f64>(data_array.float64.unwrap(), shape)
            },
        };
    
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
        Ok(())
    }
}