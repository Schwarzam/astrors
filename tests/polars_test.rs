mod common;

use std::fs::File;
use astrors::io::hdus::primaryhdu::PrimaryHDU;
use polars::frame::DataFrame;
use std::io::Result;

use polars::series::Series;
use polars::prelude::*;

use ndarray::prelude::*;

use astrors::io::hdus::bintable::bintablehdu::BinTableHDU;
use std::io::Seek;


#[test]
pub fn bintablehdu_test() -> Result<()> {
    let testfile = common::get_testdata_path("EUVEngc4151imgx.fits");
    let mut f: File = File::open(testfile)?;

    let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
    
    //Seek end_pos 
    f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

    //header.pretty_print_advanced();

    let denf = DataFrame::new(vec![
        Series::new("RA", vec![1, 2, 3, 4, 5]),
        Series::new("DEC", vec![1, 2, 3, 4, 5]),
        Series::new("MAG", vec![1, 2, 3, 4, 5]),
    ]).unwrap();
    let mut bintable = BinTableHDU::new_data(denf);

    println!("Df {:} ", bintable.data);

    let outfile = common::get_outtestdata_path("test_bintable.fits");
    let mut outf = File::create(outfile)?;

    let mut primaryhdu = PrimaryHDU::default();
    primaryhdu.write_to_file(&mut outf)?;
    bintable.write_to_file(&mut outf)?;
    

    Ok(())
}

#[test]
pub fn type_dataframe(){

    let u8vec : Vec<u8> = vec![1, 2, 3, 4, 5];

    let s0 = Series::new("days", u8vec);
    
    println!("{:?}", s0.u8().unwrap());
        
}

#[test]
pub fn teste_multifield(){
    let array: Array2<f64> = array![[1., 2.], [3., 4.], [5., 6.]];
    let mut chunked_builder = ListPrimitiveChunkedBuilder::<Float64Type>::new(
        "my_series",
        array.len_of(Axis(0)),
        array.len_of(Axis(1)),
        DataType::Float64,
    );

    for row in array.axis_iter(Axis(0)) {
        println!("Row: {:?}", row);
        match row.as_slice() {
            Some(row) => chunked_builder.append_slice(row),
            None => chunked_builder.append_slice(&row.to_vec()),
        }
    }

    let series = chunked_builder.finish().into_series();

    println!("dtype {:?}", series.dtype());

    match series.dtype(){
        DataType::List(dt) => {
            println!("List {:?}", dt.to_physical());
            match dt.to_physical(){
                DataType::Float64 => {
                    println!("Float64");
                },
                _ => {
                    println!("Not Float64");
                }
            }
        },
        _ => {
            println!("Not List");
        }
    }

    // Iterating over the series and extracting the first and second elements
    if let Ok(list) = series.list() {
        for opt_s in list.into_iter() {
            if let Some(s) = opt_s {
                // Assuming each sublist contains at least 2 elements.
                if let Ok(sub_series) = s.f64() {
                    if sub_series.len() >= 2 {
                        let first = sub_series.get(0);
                        let second = sub_series.get(1);
                        println!("First: {:?}, Second: {:?}", first, second);
                    }
                }
            }
        }
    }

    let df = DataFrame::new(vec![series]).unwrap();

    // println!("{:?}", df);
}