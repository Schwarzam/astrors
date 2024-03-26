use polars::prelude::*; // Polars library

pub fn series_to_vec_bool(series: &Series) -> Result<Vec<bool>, PolarsError> {
    series.bool().map(|ca| ca.into_iter().collect::<Vec<Option<bool>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
        .map_err(|e| e.into())
}

pub fn series_to_vec_u8(series: &Series) -> Result<Vec<u8>, PolarsError> {
    series.u8().map(|ca| ca.into_iter().collect::<Vec<Option<u8>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
        .map_err(|e| e.into())
}

pub fn series_to_vec_i8(series: &Series) -> Result<Vec<i8>, PolarsError> {
    series.i8().map(|ca| ca.into_iter().collect::<Vec<Option<i8>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
        .map_err(|e| e.into())
}

pub fn series_to_vec_i16(series: &Series) -> Result<Vec<i16>, PolarsError> {
    series.i16().map(|ca| ca.into_iter().collect::<Vec<Option<i16>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
        .map_err(|e| e.into())
}

pub fn series_to_vec_i32(series: &Series) -> Result<Vec<i32>, PolarsError> {
    series.i32().map(|ca| ca.into_iter().collect::<Vec<Option<i32>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
        .map_err(|e| e.into())
}

pub fn series_to_vec_i64(series: &Series) -> Result<Vec<i64>, PolarsError> {
    series.i64().map(|ca| ca.into_iter().collect::<Vec<Option<i64>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
        .map_err(|e| e.into())
}

pub fn series_to_vec_f32(series: &Series) -> Result<Vec<f32>, PolarsError> {
    series.f32().map(|ca| ca.into_iter().collect::<Vec<Option<f32>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or(0.0))
        .collect())
        .map_err(|e| e.into())
}

pub fn series_to_vec_f64(series: &Series) -> Result<Vec<f64>, PolarsError> {
    series.f64().map(|ca| ca.into_iter().collect::<Vec<Option<f64>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or(0.0))
        .collect())
        .map_err(|e| e.into())
}

pub fn series_to_vec_string(series: &Series) -> Result<Vec<String>, PolarsError> {
    series.str().map(|ca| ca.into_iter()
        .map(|opt| opt.map(|s| s.to_string())) // Convert &str to String
        .collect::<Vec<Option<String>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default()) // Handle nulls
        .collect())
        .map_err(|e| e.into())
}