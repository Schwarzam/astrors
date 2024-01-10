use std::{fs::File, io::{Read, Write}};

use crate::io::{Header, header::card::Card};

use polars::prelude::*; // Polars library

fn get_tform_type_size(tform: &str) -> (char, usize) {
    let tform = tform.trim();
    if tform.len() == 1 {
        (tform.chars().next().unwrap(), 1)
    } else {
        let (type_char, size_str) = tform.split_at(1);
        let size = size_str.split('.').next().unwrap().parse::<usize>().unwrap_or(1);
        (type_char.chars().next().unwrap(), size)
    }
}

#[derive(Debug, PartialEq)]
pub enum Data {
    I(Vec<i32>),
    E(Vec<f32>),
    D(Vec<f64>),
    A(Vec<String>),
    F(Vec<f32>),
}

impl Data {
    pub fn push(&mut self, element: String, data_type: char) {
        match data_type {
            'I' => {
                let element = element.to_string().parse::<i32>().unwrap();
                match self {
                    Data::I(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'E' => {
                let element = element.to_string().parse::<f32>().unwrap();
                match self {
                    Data::E(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'D' => {
                let element = element.to_string().parse::<f64>().unwrap();
                match self {
                    Data::D(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'A' => {
                match self {
                    Data::A(data) => data.push(element.to_string()),
                    _ => panic!("Wrong data type"),
                }
            }
            'F' => {
                let element = element.to_string().parse::<f32>().unwrap();
                match self {
                    Data::F(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            _ => {
                //treat as string
                match self {
                    Data::A(data) => data.push(element.to_string()),
                    _ => panic!("Wrong data type"),
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Data::I(data) => data.len(),
            Data::E(data) => data.len(),
            Data::D(data) => data.len(),
            Data::A(data) => data.len(),
            Data::F(data) => data.len(),
        }
    }

    pub fn max_len(&self) -> usize {
        match self {
            Data::I(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::E(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::D(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
            Data::A(data) => data.iter().map(|x| x.len()).max().unwrap_or(0),
            Data::F(data) => data.iter().map(|x| x.to_string().len()).max().unwrap_or(0),
        }
    }

}

#[derive(Debug)]
pub struct Column {
    ttype: String, 
    tform: String,
    tunit: Option<String>,
    tdisp: Option<String>,
    tbcol: Option<i32>,
    data: Data,
}

pub fn read_tableinfo_from_header(header: &Header) -> Result<Vec<Column>, String> {
    let mut columns: Vec<Column> = Vec::new();
    let tfields = header["TFIELDS"].value.as_int().unwrap_or(0);

    for i in 1..=tfields {
        let ttype = header.get_card(&format!("TTYPE{}", i));
        let tform = header.get_card(&format!("TFORM{}", i));
        let tunit = header.get_card(&format!("TUNIT{}", i));
        let tdisp = header.get_card(&format!("TDISP{}", i));
        let tbcol = header.get_card(&format!("TBCOL{}", i));

        if ttype.is_none() {
            break;
        }

        let ttype = ttype.unwrap().value.to_string();
        let tform = tform.unwrap().value.to_string();
        let tunit = tunit.map(|c| c.value.to_string());
        let tdisp = tdisp.map(|c| c.value.to_string());
        let tbcol = tbcol.map(|c| c.value.to_string().parse::<i32>().unwrap());

        let tform2 = tform.clone();
        let column = Column {
            ttype,
            tform,
            tunit,
            tdisp,
            tbcol,
            data : match get_tform_type_size(&tform2) {
                ('I', size) => Data::I(Vec::new()),
                ('E', size) => Data::E(Vec::new()),
                ('D', size) => Data::D(Vec::new()),
                ('A', size) => Data::A(Vec::new()),
                ('F', size) => Data::F(Vec::new()),
                (_, size) => Data::A(Vec::new()),
            }
        };

        columns.push(column);
    }

    Ok(columns)
}

pub fn columns_to_polars(columns: Vec<Column>) -> Result<DataFrame, String> {
    let mut polars_columns: Vec<Series> = Vec::new();
    for column in columns {
        let series = match column.data {
            Data::I(data) => Series::new(&column.ttype, data),
            Data::E(data) => Series::new(&column.ttype, data),
            Data::D(data) => Series::new(&column.ttype, data),
            Data::A(data) => Series::new(&column.ttype, data),
            Data::F(data) => Series::new(&column.ttype, data),
        };
        polars_columns.push(series);
    }

    let df = DataFrame::new(polars_columns).map_err(|e| e.to_string())?;
    Ok(df)
}

fn series_to_vec_i32(series: &Series) -> Result<Vec<i32>, PolarsError> {
    series.i32().map(|ca| ca.into_iter().collect::<Vec<Option<i32>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default())
        .collect())
        .map_err(|e| e.into())
}

fn series_to_vec_f32(series: &Series) -> Result<Vec<f32>, PolarsError> {
    series.f32().map(|ca| ca.into_iter().collect::<Vec<Option<f32>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or(0.0))
        .collect())
        .map_err(|e| e.into())
}

fn series_to_vec_string(series: &Series) -> Result<Vec<String>, PolarsError> {
    series.str().map(|ca| ca.into_iter()
        .map(|opt| opt.map(|s| s.to_string())) // Convert &str to String
        .collect::<Vec<Option<String>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or_default()) // Handle nulls
        .collect())
        .map_err(|e| e.into())
}

fn series_to_vec_f64(series: &Series) -> Result<Vec<f64>, PolarsError> {
    series.f64().map(|ca| ca.into_iter().collect::<Vec<Option<f64>>>()
        .into_iter()
        .map(|opt| opt.unwrap_or(0.0))
        .collect())
        .map_err(|e| e.into())
}

pub fn polars_to_columns_update_header(df: DataFrame) -> Result<Vec<Column>, std::io::Error> {
    let mut columns: Vec<Column> = Vec::new();
    for series in df.get_columns() {
        let data = match series.dtype() {
            DataType::Int32 => Data::I(series_to_vec_i32(series).unwrap()),
            DataType::Float32 => Data::E(series_to_vec_f32(series).unwrap()),
            DataType::Float64 => Data::D(series_to_vec_f64(series).unwrap()),
            DataType::String => Data::A(series_to_vec_string(series).unwrap()),
            _ => Data::A(series_to_vec_string(series).unwrap()),
        };
        let column = Column {
            ttype: series.name().to_string(),
            tform: "1A".to_string(),
            tunit: None,
            tdisp: None,
            tbcol: None,
            data,
        };
        columns.push(column);
    }

    for column in columns.iter_mut() {
        let formatted_string;
        let tform = match &column.data {
            Data::I(_) => "I12",
            Data::E(_) => "E15.7",
            Data::D(_) => "D25.17",
            Data::A(data) => {
                formatted_string = format!("A{}", column.data.max_len());
                &formatted_string
            },
            Data::F(_) => "F",
        };
        column.tform = tform.to_string();
    }


    Ok(columns)
}

pub fn columns_to_buffer(columns: Vec<Column>, file: &mut File) -> Result<(), std::io::Error> {
    //buffer should be written in utf8
    let rows = columns[0].data.len();

    for row in 0..rows {
        for column in columns.iter() {
            
            let data = match &column.data {
                Data::I(data) => data[row].to_string(),
                Data::E(data) => data[row].to_string(),
                Data::D(data) => data[row].to_string(),
                Data::A(data) => data[row].to_string(),
                Data::F(data) => data[row].to_string(),
            };

            let mut buffer = data.as_bytes().to_vec();
            

            file.write_all(&buffer)?;
        }
    }
    Ok(())

}

pub fn fill_columns_w_data(columns : &mut Vec<Column>, nrows: i64, file: &mut File) -> Result<(), std::io::Error> {
    for row in 1..=nrows{
        for column in columns.iter_mut() {
            let (data_type, size) = get_tform_type_size(&column.tform);
    
            let mut buffer = vec![0; size + 1];
            file.read_exact(&mut buffer)?;
    
            column.data.push(String::from_utf8_lossy(&buffer).trim_end().trim_start().to_string(), data_type);
        }
    }
    Ok(())
}
