use std::fs::File;
use std::io::Read;

use crate::io::header::{card::CardValue, Header};

use ndarray::ArrayBase;

use std::io::{BufWriter, Write};

use crate::io::hdus::image::utils::{
    get_shape, nbytes_from_bitpix, pre_bytes_to_f32_vec, pre_bytes_to_f64_vec,
    pre_bytes_to_i16_vec, pre_bytes_to_i32_vec, pre_bytes_to_u8_vec, vec_to_ndarray,
};
use ndarray::ArrayD;
use rayon::prelude::*;

pub enum ImageData {
    U8(ArrayD<u8>),
    I16(ArrayD<i16>),
    I32(ArrayD<i32>),
    F32(ArrayD<f32>),
    F64(ArrayD<f64>),
    EMPTY,
}

impl ImageData {
    pub fn new() -> Self {
        ImageData::EMPTY
    }

    pub fn get_bitpix(&self) -> i32 {
        match self {
            ImageData::U8(_) => 8,
            ImageData::I16(_) => 16,
            ImageData::I32(_) => 32,
            ImageData::F32(_) => -32,
            ImageData::F64(_) => -64,
            _ => 8,
        }
    }

    pub fn get_dtype(&self) -> String {
        match self {
            ImageData::U8(_) => String::from("uint8"),
            ImageData::I16(_) => String::from("int16"),
            ImageData::I32(_) => String::from("int32"),
            ImageData::F32(_) => String::from("float32"),
            ImageData::F64(_) => String::from("float64"),
            _ => String::from("uint8"),
        }
    }

    pub fn get_shape(&self) -> Vec<usize> {
        match self {
            ImageData::U8(array) => array.shape().to_vec(),
            ImageData::I16(array) => array.shape().to_vec(),
            ImageData::I32(array) => array.shape().to_vec(),
            ImageData::F32(array) => array.shape().to_vec(),
            ImageData::F64(array) => array.shape().to_vec(),
            _ => vec![0, 0],
        }
    }
}

pub struct ImData<T> {
    pub data: ArrayD<T>,
}

use std::fmt;

impl fmt::Debug for ImageData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ImageData::U8(array) => {
                write!(f, "FitsData::U8({:?})", array)
            }
            ImageData::I16(array) => {
                write!(f, "FitsData::I16({:?})", array)
            }
            ImageData::I32(array) => {
                write!(f, "FitsData::I32({:?})", array)
            }
            ImageData::F32(array) => {
                write!(f, "FitsData::F32({:?})", array)
            }
            ImageData::F64(array) => {
                write!(f, "FitsData::F64({:?})", array)
            }
            _ => write!(f, "FitsData::EMPTY"),
        }
    }
}

pub struct ImageParser;

impl ImageParser {
    //TODO: Find where to implement BZERO and BSCALE
    pub fn calculate_image_bytes(header: &Header) -> usize {
        let bitpix: i32 = header["BITPIX"].value.as_int().unwrap_or(0) as i32;
        let shape = get_shape(header).unwrap();
        let dtype_bytes = nbytes_from_bitpix(bitpix);
        shape.iter().fold(1, |acc, x| acc * x) * dtype_bytes
    }

    pub fn read_from_buffer(
        f: &mut File,
        header: &mut Header,
    ) -> Result<ImageData, std::io::Error> {
        let _naxis: usize = header["NAXIS"].value.as_int().unwrap_or(0) as usize;

        let bitpix: i32 = header["BITPIX"].value.as_int().unwrap_or(0) as i32;
        let shape = get_shape(header)?;

        // Get data type from BITPIX
        let dtype_bytes = nbytes_from_bitpix(bitpix);

        let total_bytes = shape.iter().fold(1, |acc, x| acc * x) * dtype_bytes;
        let mut databuf = vec![0; total_bytes];
        let _ = f.read(&mut databuf)?;

        // Read until the end of the current FITS block
        let remainder = total_bytes % 2880;
        if remainder != 0 {
            let mut padding = vec![0; 2880 - remainder];
            let _ = f.read(&mut padding)?;
            // println!("Padding: {:?}", padding.len());
        }

        ImageParser::image_buffer_to_ndarray(&databuf, shape, bitpix)
    }

    pub fn image_buffer_to_ndarray(
        databuf: &Vec<u8>,
        shape: Vec<usize>,
        bitpix: i32,
    ) -> Result<ImageData, std::io::Error> {
        match bitpix {
            8 => {
                let mut vect: Vec<u8> = vec![0; databuf.len() / 1];
                pre_bytes_to_u8_vec(&databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let data: ImageData = ImageData::U8(ndarray);
                Ok(data)
            }
            16 => {
                let mut vect: Vec<i16> = vec![0; databuf.len() / 2];
                pre_bytes_to_i16_vec(&databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let data = ImageData::I16(ndarray);
                Ok(data)
            }
            32 => {
                let mut vect: Vec<i32> = vec![0; databuf.len() / 4];
                pre_bytes_to_i32_vec(&databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let data = ImageData::I32(ndarray);
                Ok(data)
            }
            -32 => {
                let mut vect: Vec<f32> = vec![0.0; databuf.len() / 4];
                pre_bytes_to_f32_vec(&databuf, &mut vect);
                let ndarray: ArrayBase<ndarray::OwnedRepr<f32>, ndarray::Dim<ndarray::IxDynImpl>> =
                    vec_to_ndarray(vect, shape);
                let data = ImageData::F32(ndarray);
                Ok(data)
            }
            -64 => {
                let mut vect: Vec<f64> = vec![0.0; databuf.len() / 8];
                pre_bytes_to_f64_vec(&databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let data = ImageData::F64(ndarray);
                Ok(data)
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Not implemented",
            )),
        }
    }

    pub fn ndarray_to_buffer_parallel(data: &ImageData) -> Vec<u8> {
        match data {
            ImageData::U8(array) => {
                let vect = array.clone().into_raw_vec();
                vect.par_iter()
                    .flat_map(|&item| item.to_be_bytes().to_vec())
                    .collect::<Vec<u8>>()
            }
            ImageData::I16(array) => {
                let vect = array.clone().into_raw_vec();
                vect.par_iter()
                    .flat_map(|&item| item.to_be_bytes().to_vec())
                    .collect::<Vec<u8>>()
            }
            ImageData::I32(array) => {
                let vect = array.clone().into_raw_vec();
                vect.par_iter()
                    .flat_map(|&item| item.to_be_bytes().to_vec())
                    .collect::<Vec<u8>>()
            }
            ImageData::F32(array) => {
                let vect = array.clone().into_raw_vec();
                vect.par_iter()
                    .flat_map(|&item| item.to_be_bytes().to_vec())
                    .collect::<Vec<u8>>()
            }
            ImageData::F64(array) => {
                let vect = array.clone().into_raw_vec();
                vect.par_iter()
                    .flat_map(|&item| item.to_be_bytes().to_vec())
                    .collect::<Vec<u8>>()
            }
            _ => vec![],
        }
    }

    pub fn write_image_header(header: &mut Header, data: &ImageData) {
        let shape = data.get_shape();
        let mut naxis = shape.len();
        if shape.eq(&vec![0, 0]) {
            naxis = 0;
        }

        let bitpix = data.get_bitpix();
        header["BITPIX"].value = CardValue::INT(bitpix as i64);
        header["NAXIS"].value = CardValue::INT(naxis as i64);

        for i in 0..naxis {
            let naxisn = format!("NAXIS{}", i + 1);
            header[naxisn.as_str()].value = CardValue::INT(shape[i] as i64);
        }

        //if other NAXISn keywords are present, remove them
        for i in naxis + 1..=7 {
            let naxisn = format!("NAXIS{}", i);
            if header.contains_key(naxisn.as_str()) {
                header.remove(naxisn.as_str());
            }
        }
    }

    pub fn write_to_buffer(
        data: &ImageData,
        mut writer: impl std::io::Write,
    ) -> std::io::Result<()> {
        let mut buffer = ImageParser::ndarray_to_buffer_parallel(&data);
        let remainder = buffer.len() % 2880;
        if remainder != 0 {
            let padding = vec![0; 2880 - remainder];
            buffer.extend(padding);
        }
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn ndarray_to_buffer<W: Write>(data: &ImageData, writer: W) -> std::io::Result<()> {
        let mut writer = BufWriter::new(writer);
        let mut bytes_written = 0;
        match data {
            ImageData::U8(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 1] = item.to_be_bytes();
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            }
            ImageData::I16(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 2] = item.to_be_bytes();
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            }
            ImageData::I32(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 4] = item.to_be_bytes();
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            }
            ImageData::F32(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 4] = f32::to_be_bytes(item);
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            }
            ImageData::F64(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 8] = f64::to_be_bytes(item);
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            }
            _ => bytes_written += 0,
        }
        let remainder = bytes_written % 2880;
        if remainder != 0 {
            let padding = vec![0; 2880 - remainder];
            // bytes_written += padding.len(); // increment the counter
            writer.write_all(&padding)?;
        }
        writer.flush()
    }
}
