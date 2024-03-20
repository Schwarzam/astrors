// use polars_core::prelude::*;


#[cfg(test)]
mod tablehdu_tests {
    use polars::prelude::*;
    use astrors::io::hdus::table::rw_bintable::{prelude::CsvReader, SerReader};

    #[test]
    fn test_tablehdu() -> PolarsResult<()> {

        let file_path = "/Users/gustavo/Downloads/SPLUS_DR4_stparam_SPHINX_v1.csv";
        use std::path::PathBuf;
        let path = PathBuf::from(file_path);
        let df = CsvReader::from_path(path)?.finish()?;

        // Print the first few rows of the DataFrame.
        println!("{:?}", df.head(Some(5)));

        Ok(())

    }
}