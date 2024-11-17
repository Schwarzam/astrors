
use astrors::io::hdus::bintable::bintablehdu::BinTableHDU;
use astrors::io::hdus::primaryhdu::PrimaryHDU;

use std::fs::File;
use std::io::{Result, Seek};

mod common;

#[test]
fn test_read_fits() -> Result<()> {

    let file = common::get_testdata_path("0.1_0.1_300_R_swp_splus.fits.fz");

    let mut f: File = File::open(file)?;

    let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
    //Seek end_pos 
    f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

    let bintable = BinTableHDU::read_from_file(&mut f).unwrap();

    println!("{:?}", bintable.data);

    // read_tableinfo_from_header()
    Ok(())
}

#[test]
fn compress_random_image() -> Result<()> {
    

    // let mut rng = rand::thread_rng();
    // let mut data = Array::from_shape_fn((1000, 1000), |_| rng.gen::<f32>());
    // let compressed = compress_image(data);

    // read_tableinfo_from_header()
    Ok(())
}


// #[cfg(test)]
// mod test_compression_alg {
//     use std::ffi::CString;
//     use std::os::raw::c_char;
//     use std::any::type_name;
//     use std::mem;

//     use rand::{self, Rng};
//     use astrors::io::compression_algorithms::*;

//     #[test]
//     fn hcompress() {
//         // create random vec of 300 elements random ints 0 - 100 range
//         let mut rng = rand::thread_rng();
//         let mut a = Vec::with_capacity(300);
//         for _ in 0..300 {
//             a.push(rng.gen_range(0..100));
//         }

//         println!("a = {:?}", a.len());
//         let _tamanho = a.len() as i32;
    
//         let mut nbytes : i64 = 300 * 4;
//         let mut status : i32 = 0;
//         // let mut output : char = String::new();
    
//         let mut output_buffer: Vec<u8> = vec![0; nbytes as usize];
//         // let output_ptr = output_buffer.as_mut_ptr() as *mut c_char;
//         // std::mem::forget(output_buffer); // Prevent buffer from being deallocated
//         println!("a = {:?}", a);

//         let mut scale : i32 = 0;
//         unsafe {
//             fits_hcompress(a.as_mut_ptr().cast(), 300, 1, scale, output_buffer.as_mut_ptr().cast(), &mut nbytes, &mut status);
//         }
        
//         println!("nbytes = {:?}", nbytes);
        
//         // let output_string = unsafe { CString::from_raw(output_ptr) };
//         // let output_string = output_string.to_str().expect("Invalid UTF-8");
//         // println!("output_string = {:?}", output_string);

//         let mut b = [0; 300];
//         let mut output_buffer2: Vec<u8> = output_buffer[0..nbytes as usize].to_vec();

//         unsafe {
//             fits_hdecompress(output_buffer2.as_mut_ptr().cast(), 0, b.as_mut_ptr().cast(), &mut 300, &mut 1, &mut scale, &mut status);
//         }
        
//         println!("b = {:?}", b);
            
//         }

    
// }