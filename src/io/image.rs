use std::fs::File;
use std::io::Read;
use crate::io::header::Header;

use memmap::Mmap;


use crate::io::aux::{bytes_to_f32_vec, bytes_to_f64_vec, vec_to_ndarray, DataType};
use ndarray::{ArrayD, IxDyn};

 

pub struct Data {
    data: ndarray::ArrayD<DataType>,
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

        // Create a memory-mapped file
        let mmap = unsafe { Mmap::map(&f)? };
        let databuf = mmap[..naxis1 * naxis2 * nbytes].to_vec();
        // let mut databuf = vec![0; naxis1 * naxis2 * nbytes];
        
        // let data = f.read(&mut databuf)?;
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        let vect = bytes_to_f64_vec(&databuf);
        // println!("vect: {:?}", vect[0..10].to_vec());
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);

        let shape = vec![naxis1, naxis2];
        let ndarray = vec_to_ndarray(vect, shape); 


        // println!("{:?}", ndarray); 
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
        Ok(())
    }
}