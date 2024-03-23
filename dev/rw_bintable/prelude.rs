#[cfg(test)]
use polars_core::prelude::*;

pub use crate::io::hdus::table::rw_bintable::csv::*;

pub use crate::io::hdus::table::rw_bintable::utils::*;
pub use crate::io::hdus::table::rw_bintable::{SerReader, SerWriter};



#[cfg(test)]
pub(crate) fn create_df() -> DataFrame {
    let s0 = Series::new("days", [0, 1, 2, 3, 4].as_ref());
    let s1 = Series::new("temp", [22.1, 19.9, 7., 2., 3.].as_ref());
    DataFrame::new(vec![s0, s1]).unwrap()
}
