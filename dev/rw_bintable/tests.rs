//! tests that require parsing a csv
//!

use polars_core::prelude::*;

use crate::io::hdus::table::rw_bintable::csv::CsvReader;
use crate::SerReader;

#[test]
fn test_filter() -> PolarsResult<()> {
    println!("running test");
    let path = "/Users/gustavo/Downloads/data-1701699078659.csv";
    let df = CsvReader::from_path(path)?.finish()?;

    let out = df.filter(&df.column("fats_g")?.gt(4)?)?;

    // this fails if all columns are not equal.
    println!("{out}");

    Ok(())
}
