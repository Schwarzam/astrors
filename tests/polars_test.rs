mod common;

use std::{fs::File, io::Read};
use astrors::io::Header;
use astrors::io::hdus::primaryhdu::PrimaryHDU;

use astrors::io::hdus::bintable::bintable::*;

use astrors::io::hdus::bintable::buffer::ColumnDataBuffer;
use astrors::io::hdus::bintable::bintablehdu::BinTableHDU;


#[cfg(test)]
/// Module containing tests for the `tablehdu` module.
mod tablehdu_tests {
    use std::fs::File;


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
    fn split_buffer(buffer_size: usize, n: u16, row_size: u16) -> Vec<(usize, usize)> {
        let mut limits = Vec::new();
        let mut start: usize = 0;
        let mut end: usize;

        let nbufs = buffer_size / n as usize;
        for i in 0..n {
            if n - 1 == i {
                end = buffer_size;
            } else {
                if (start + nbufs) % row_size as usize != 0 {
                    end = start + nbufs + row_size as usize - (start + nbufs) % row_size as usize;
                } else {
                    end = start + nbufs;
                }
            }
            limits.push((start, end));
            start = end;
        }
        limits
    }

    use std::{io::{Write, Seek}, ops::Mul, fmt::Error};
    use std::io::Result;
    use polars;
    use polars::frame::DataFrame;
    use super::*;
    use astrors::io::hdus::bintable::*;
    #[test]
    fn test_tablehdu() -> Result<()> {
        // Get CPUs count
        let n_chunks = 8;
        let n_threads = 8;
        
        let testfile = common::get_testdata_path("/Users/gustavo/Downloads/SPLUS_DR4_stparam_SPHINX_v1.fits");
        
        //let testfile = common::get_testdata_path("EUVEngc4151imgx.fits");
        let mut f: File = File::open(testfile)?;
    
        let end_pos = PrimaryHDU::get_end_byte_position(&mut f);
        f.seek(std::io::SeekFrom::Start(end_pos as u64))?;

        let mut header = Header::new();
        header.read_from_file(&mut f)?;
        
        let columns = read_tableinfo_from_header(&header).unwrap();
        //println!("Columns: {:?}", columns);

        let nrows = header["NAXIS2"].value.as_int().unwrap_or(0);

        let bytes_per_row = calculate_number_of_bytes_of_row(&columns);
        let buffer_size = nrows as usize * bytes_per_row;
        let limits = split_buffer(buffer_size, n_chunks, bytes_per_row as u16);
        
        let mut buffer = vec![0; buffer_size];
        f.read_exact(&mut buffer)?;
        
        use rayon::prelude::*;
        //use rayon pool install
        //let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap();
        let pool = rayon::ThreadPoolBuilder::new().num_threads(n_threads as usize).build().unwrap();
        let mut results : Vec<DataFrame> = pool.install(|| {
            limits.into_par_iter().map(|(start, end)| {
                let local_buffer: &[u8] = &buffer[start..end];
                let nbuffer_rows = (end - start) / bytes_per_row;

                let mut local_buf_cols : Vec<ColumnDataBuffer> = Vec::new();
                columns.iter().for_each(|column: &Column| {
                    local_buf_cols.push(ColumnDataBuffer::new(&column.tform, nbuffer_rows as i32));
                });
                
                (0..nbuffer_rows).into_iter().for_each(|i| {
                    let mut offset = 0;
                    let row_start_idx = i * bytes_per_row;

                    let row = &local_buffer[row_start_idx..row_start_idx + bytes_per_row];

                    columns.iter().enumerate().for_each(|(j, column)| {
                        let buf_col = &mut local_buf_cols[j];
                        let col_bytes = &row[column.start_address..column.start_address + column.type_bytes];
                        buf_col.write_on_idx(col_bytes, column.char_type, i as i64);
                        offset += column.type_bytes;
                    });
                });

                let df_cols = columns.iter().enumerate().map(|(i, column)| {
                    let buf_col = &local_buf_cols[i];
                    let series = buf_col.to_series(&column.ttype);
                    local_buf_cols[i].clear();
                    series
                }).collect();

                let local_df = unsafe { DataFrame::new_no_checks(df_cols) };
                local_df
            }).collect()
        });

        let mut final_df = results[0].clone();
        for i in 1..results.len() {
            final_df.vstack_mut(&results[i]);
            // results.remove(i);
        }

        println!("Final DF: {}", final_df);

        Ok(())
    }
}