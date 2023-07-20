use ndarray::{ArrayD, IxDyn};

fn vec_to_ndarray(data: Vec<f32>, shape: Vec<usize>) -> ArrayD<f32> {
    let shape_ix = IxDyn(&shape);
    ArrayD::from_shape_vec(shape_ix, data).unwrap() // handle the error appropriately in your code
}

fn bytes_to_f32_vec(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks(4)
        .map(|b| f32::from_bits(u32::from_be_bytes([b[0], b[1], b[2], b[3]])))
        .collect()
}

mod io {
    pub mod header;
    pub mod image;
    pub mod aux;
}

#[test]
fn read_test() -> std::io::Result<()>{
    // crate::fits_io::read_file();

    use std::fs::File;
    let mut f = File::open("./testdata/test.fits")?;

    let mut header = crate::io::header::Header::new();
    header.read_from_filebytes(&mut f)?;

    header.pretty_print();
    
    crate::io::image::Data::read_from_filebytes(&mut f, &mut header)?;

    Ok(())
}
