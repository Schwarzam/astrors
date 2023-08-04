use std::fs::File;
use std::io::Read;

use crate::io::header::Header;

use rayon::prelude::*;
use ndarray::ArrayBase;
use byteorder::{ByteOrder, NativeEndian};

use std::io::{Write, BufWriter};

use crate::io::utils::{
    get_shape,
    pre_bytes_to_f64_vec,
    pre_bytes_to_f32_vec,
    pre_bytes_to_u8_vec,
    pre_bytes_to_i16_vec,
    pre_bytes_to_i32_vec, 
    vec_to_ndarray, 
    nbytes_from_bitpix
};
use ndarray::ArrayD;

pub enum FitsData {
    U8(ArrayD<u8>),
    I16(ArrayD<i16>),
    I32(ArrayD<i32>),
    F32(ArrayD<f32>),
    F64(ArrayD<f64>),
}

use std::fmt;

impl fmt::Debug for FitsData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FitsData::U8(array) => {
                write!(f, "FitsData::U8({:?})", array)
            },
            FitsData::I16(array) => {
                write!(f, "FitsData::I16({:?})", array)
            },
            FitsData::I32(array) => {
                write!(f, "FitsData::I32({:?})", array)
            },
            FitsData::F32(array) => {
                write!(f, "FitsData::F32({:?})", array)
            },
            FitsData::F64(array) => {
                write!(f, "FitsData::F64({:?})", array)
            },
            _ => {
                panic!("Not implemented");
            }
        }
    }
}
pub struct FitsParser;

impl FitsParser {
    pub fn read_from_buffer(f: &mut File, header: &mut Header) -> Result<FitsData, std::io::Error>  {
        let _naxis: usize = header.parse_header_value("NAXIS")?;
    
        let bitpix : i32 = header.parse_header_value("BITPIX")?;
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

        FitsParser::image_buffer_to_ndarray(databuf, shape, bitpix) 
    }


    pub fn image_buffer_to_ndarray(databuf: Vec<u8>, shape: Vec<usize>, bitpix: i32) -> Result<FitsData, std::io::Error>  {
        match bitpix {
            8 => {
                let mut vect: Vec<u8> = vec![0; databuf.len() / 1];
                pre_bytes_to_u8_vec(databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let mut data: FitsData = FitsData::U8(ndarray);
                Ok(data)
            },
            16 => {
                let mut vect: Vec<i16> = vec![0; databuf.len() / 2];
                pre_bytes_to_i16_vec(databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let mut data = FitsData::I16(ndarray);
                Ok(data)
            },
            32 => {
                let mut vect: Vec<i32> = vec![0; databuf.len() / 4];
                pre_bytes_to_i32_vec(databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let mut data = FitsData::I32(ndarray);
                Ok(data)
            },
            -32 => {
                let mut vect: Vec<f32> = vec![0.0; databuf.len() / 4];
                pre_bytes_to_f32_vec(databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let mut data = FitsData::F32(ndarray);
                Ok(data)
            },
            -64 => {
                let mut vect: Vec<f64> = vec![0.0; databuf.len() / 8];
                pre_bytes_to_f64_vec(databuf, &mut vect);
                let ndarray = vec_to_ndarray(vect, shape);
                let mut data = FitsData::F64(ndarray);
                Ok(data)
            },
            _ => {
                panic!("Not implemented");
            }   
        }
    }

    pub fn ndarray_to_buffer_parallel(data: &FitsData) -> Vec<u8> {
        match data {
            FitsData::U8(array) => {
                let mut vect = array.clone().into_raw_vec();
                vect.par_iter().flat_map(|&item| item.to_ne_bytes().to_vec()).collect::<Vec<u8>>()
            },
            FitsData::I16(array) => {
                let mut vect = array.clone().into_raw_vec();
                vect.par_iter().flat_map(|&item| item.to_ne_bytes().to_vec()).collect::<Vec<u8>>()
            },
            FitsData::I32(array) => {
                let mut vect = array.clone().into_raw_vec();
                vect.par_iter().flat_map(|&item| item.to_ne_bytes().to_vec()).collect::<Vec<u8>>()
            },
            FitsData::F32(array) => {
                let mut vect = array.clone().into_raw_vec();
                vect.par_iter().flat_map(|&item| item.to_ne_bytes().to_vec()).collect::<Vec<u8>>()
            },
            FitsData::F64(array) => {
                let mut vect = array.clone().into_raw_vec();
                vect.par_iter().flat_map(|&item| item.to_ne_bytes().to_vec()).collect::<Vec<u8>>()
            },
            _ => {
                panic!("Not implemented");
            }
        }
    }

    pub fn write_to_buffer(data : &FitsData, mut writer: impl std::io::Write) -> std::io::Result<()> {
        let mut buffer = FitsParser::ndarray_to_buffer_parallel(&data);
        let remainder = buffer.len() % 2880;
        if remainder != 0 {
            let padding = vec![0; 2880 - remainder];
            buffer.extend(padding);
        }
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn ndarray_to_buffer<W: Write>(data: FitsData, mut writer: W) -> std::io::Result<()> {
        let mut writer = BufWriter::new(writer);
        let mut bytes_written = 0;
        match data {
            FitsData::U8(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 1] = item.to_be_bytes();
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            },
            FitsData::I16(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 2] = item.to_be_bytes();
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            },
            FitsData::I32(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 4] = item.to_be_bytes();
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            },
            FitsData::F32(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 4] = f32::to_be_bytes(item);
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            },
            FitsData::F64(ndarray) => {
                for &item in ndarray.iter() {
                    let bytes: [u8; 8] = f64::to_be_bytes(item);
                    writer.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
            },
            _ => {
                panic!("Not implemented");
            }
        }
        let remainder = bytes_written % 2880;
        if remainder != 0 {
            let padding = vec![0; 2880 - remainder];
            bytes_written += padding.len(); // increment the counter
            writer.write_all(&padding)?;
        }
        writer.flush()
    }
}

#[test]
fn read_image_test() -> std::io::Result<()>{
    // crate::fits_io::read_file();
    use crate::{GLOBAL_FILE_NAME, WRITE_FILE};

    use std::time::Instant;
    let now = Instant::now();

    rayon::ThreadPoolBuilder::new().num_threads(8).build_global().unwrap();

    use std::fs::File;
    // let mut f = File::open("./testdata/test.fits")?
    let mut f: File = File::open(GLOBAL_FILE_NAME.as_str())?;

    let mut header = crate::io::header::Header::new();
    header.read_from_file(&mut f)?;
    header.pretty_print();

    use std::io::Write;
    // let mut file = File::create(WRITE_FILE.as_str())?;
    let mut file = File::create("output.fits")?;
    header.write_to(&mut file)?;
    
    file.flush()?;

    let mut data = crate::io::image::FitsParser::read_from_buffer(&mut f, &mut header)?;
    println!("Data: {:?}", data);

    if let FitsData::F32(ndarray) = &data {
        println!("Data Mean: {:?}", ndarray.mean());
    }
    FitsParser::ndarray_to_buffer(data, &mut file);
    
    use rayon::prelude::*;
    println!("{} threads", rayon::current_num_threads());

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    Ok(())
}
