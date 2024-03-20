// use polars_core::prelude::*;


#[cfg(test)]
mod tablehdu_tests {
    use std::fmt::Result;

    use polars::prelude::*;

    #[test]
    fn test_tablehdu() -> PolarsResult<()> {

        let file_path = "/Users/gustavo/Downloads/data-1701699078659.csv";
        let df = CsvReader::from_path(file_path)?
            .infer_schema(None) // Automatically infer the data schema
            .finish()?;
        


        // Print the first few rows of the DataFrame.
        println!("{:?}", df.head(Some(5)));

        Ok(())

    }
}