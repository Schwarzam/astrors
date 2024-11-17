/*!
 * Utilities for Handling Binary Data and FITS Image Processing
 * 
 * This module provides a collection of utility functions for efficient conversion and manipulation
 * of binary data used in FITS (Flexible Image Transport System) file processing. The utilities
 * are designed to handle raw byte buffers and convert them into numerical data types such as
 * integers and floating-point values. Additionally, it includes tools for working with multidimensional
 * data structures (e.g., images) and their metadata.
 *
 * Key functionalities include:
 * - Conversion of raw byte arrays into numerical vectors (`Vec<i8>`, `Vec<f32>`, `Vec<f64>`, etc.).
 * - Pre-allocated conversions for improved performance in large datasets.
 * - Determination of the size of data types based on the FITS `BITPIX` standard.
 * - Extraction of multidimensional shapes from FITS headers.
 * - Construction of `ndarray` structures from raw data and shapes.
 *
 * This module leverages the `rayon` crate for parallel processing, ensuring high performance
 * when handling large FITS files or datasets. These utilities are a core part of the FITS 
 * file handling pipeline, enabling both efficient data loading and memory-safe operations.
 */

use rayon::prelude::*;
use ndarray::{ArrayD, IxDyn};

use crate::io::header::Header;

pub fn nbytes_from_bitpix(bitpix : i32) -> usize {
    match bitpix {
        8 => 1,
        16 => 2,
        32 => 4,
        -32 => 4,
        -64 => 8,
        _ => panic!("Unknown bitpix value"),
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

pub fn bytes_to_f64_vec(bytes : &[u8]) -> Vec<f64> {
    bytes
        .par_chunks(8)
        .map(|b| f64::from_bits(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])))
        .collect()
}

pub fn pre_bytes_to_f64_vec(bytes: &Vec<u8>, output: &mut Vec<f64>) { // Preallocated vect
    assert!(output.len() * 8 <= bytes.len());
    output.par_iter_mut()
        .enumerate()
        .for_each(|(i, item)| {
            let chunk = &bytes[i * 8..(i+1) * 8];
            *item = f64::from_bits(u64::from_be_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3],
                chunk[4], chunk[5], chunk[6], chunk[7]
            ]));
        });
}

pub fn pre_bytes_to_f32_vec(bytes: &Vec<u8>, output: &mut Vec<f32>) {
    assert!(output.len() * 4 <= bytes.len());
    output.par_iter_mut()
        .enumerate()
        .for_each(|(i, item)| {
            let chunk = &bytes[i * 4..(i+1) * 4];
            *item = f32::from_bits(u32::from_be_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3]
            ]));
        });
}

pub fn pre_bytes_to_u8_vec(bytes: &Vec<u8>, output: &mut Vec<u8>) {
    assert!(output.len() <= bytes.len());
    output.par_iter_mut()
        .enumerate()
        .for_each(|(i, item)| {
            *item = bytes[i];
        });
}

pub fn pre_bytes_to_i16_vec(bytes: &Vec<u8>, output: &mut Vec<i16>) {
    assert!(output.len() * 2 <= bytes.len());
    output.par_iter_mut()
        .enumerate()
        .for_each(|(i, item)| {
            let chunk = &bytes[i * 2..(i+1) * 2];
            *item = i16::from_be_bytes([
                chunk[0], chunk[1]
            ]);
        });
}

pub fn pre_bytes_to_i32_vec(bytes: &Vec<u8>, output: &mut Vec<i32>) {
    assert!(output.len() * 4 <= bytes.len());
    output.par_iter_mut()
        .enumerate()
        .for_each(|(i, item)| {
            let chunk = &bytes[i * 4..(i+1) * 4];
            *item = i32::from_be_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3]
            ]);
        });
}

pub fn get_shape(header: &Header) -> Result<Vec<usize>, std::io::Error> {
    let mut shape = Vec::new();
    let naxis: usize = header["NAXIS"].value.as_int().unwrap_or(0) as usize;
    for i in 1..=naxis {
        let key = format!("NAXIS{}", i);
        let value: usize = header[&key].value.as_int().unwrap_or(0) as usize;
        shape.push(value);
    }
    Ok(shape)
}

pub fn vec_to_ndarray<T>(data: Vec<T>, shape: Vec<usize>) -> ArrayD<T> {
    let shape_ix = IxDyn(&shape);
    ArrayD::from_shape_vec(shape_ix, data).unwrap() // handle the error appropriately in your code
}