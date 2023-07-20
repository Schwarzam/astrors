use std::fs::File;
use std::io::Read;
use crate::io::header::Header;

use crate::io::aux::{bytes_to_f32_vec, vec_to_ndarray, DataType};
use ndarray::{ArrayD, IxDyn};

 

pub struct Data {
    data: ndarray::ArrayD<DataType>,
}

impl Data {

    pub fn read_from_filebytes(f: &mut File, header: &mut Header) -> std::io::Result<()> {
        
        let naxis = header.get_value("NAXIS").unwrap().parse::<usize>().unwrap();
        let naxis1 = header.get_value("NAXIS1").unwrap().parse::<usize>().unwrap();
        let naxis2 = header.get_value("NAXIS2").unwrap().parse::<usize>().unwrap();

        let bitpix : i32 = header.get_value("BITPIX").unwrap().parse::<i32>().unwrap();

        // I want to check the bitpix value with the data type 

        let dtype = DataType::from_bitpix(bitpix).unwrap();
        let nbytes = dtype.nbytes();
        println!("nbytes: {:?}", nbytes);

        let mut databuf = vec![0; naxis1 * naxis2 * nbytes];
        
        let data = f.read(&mut databuf)?;
    
        let vect = bytes_to_f32_vec(&databuf);
    
        let shape = vec![100, 100];
        let ndarray = vec_to_ndarray(vect, shape); 


        println!("{:?}", ndarray); 
        Ok(())
    }
}