use polars::prelude::*; // Polars library

pub fn series_to_vec_i32(series: &Series) -> Result<Vec<i32>, PolarsError> {
    series.i32().map(|ca| ca.into_iter().collect::<Vec<Option<i32>>>()
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

pub fn series_to_vec_string(series: &Series) -> Result<Vec<String>, PolarsError> {
    series.str().map(|ca| ca.into_iter()
        .map(|opt| opt.map(|s| s.to_string())) // Convert &str to String
        .collect::<Vec<Option<String>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default()) // Handle nulls
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

pub fn format_scientific<T>(num: T, max_len: usize) -> String 
where
    T: std::fmt::LowerExp + PartialEq + Into<f64>,
{
    let mut formatted = format!("{:.e}", num);
    if formatted.contains("0e0"){
        formatted = formatted.replace("0e0", "0.0");
    }
    formatted = formatted.replace("e", "E");
    if formatted.len() > max_len {
        formatted[..max_len].to_string()
    } else {
        formatted
    }
}