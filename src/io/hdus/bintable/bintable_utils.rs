pub fn series_to_vec_i32(series: &Series) -> Result<Vec<i32>, PolarsError> {
    series.i32().map(|ca| ca.into_iter().collect::<Vec<Option<i32>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
        .map_err(|e| e.into())
}