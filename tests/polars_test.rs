// use polars_core::prelude::*;

use arrow::buffer;
use polars::series::Series;

#[cfg(test)]
/// Module containing tests for the `tablehdu` module.
mod tablehdu_tests {
    use polars::prelude::*;
    //use astrors::io::hdus::table::rw_bintable::{prelude::CsvReader, SerReader};

    /// Splits a buffer into multiple segments of equal size.
    ///
    /// # Arguments
    ///
    /// * `buffer_size` - The size of the buffer.
    /// * `n` - The number of segments to split the buffer into.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the start and end indices of each segment.
    fn split_buffer(buffer_size: usize, n: u16) -> Vec<(usize, usize)> {
        let mut limits = Vec::new();
        let mut start: usize = 0;
        let mut end: usize;

        let nbufs = buffer_size / n as usize;
        for i in 0..n {
            if n - 1 == i {
                end = buffer_size;
            } else {
                end = start + nbufs;
            }
            limits.push((start, end));
            start = end;
        }
        limits
    }

    

    
    

    

    #[test]
    fn test_tablehdu() {
        split_buffer(10000, 3);

        let mut foo = Series::new("foo", &[1, 2, 3]);
        // push a null
        
        
    }
}