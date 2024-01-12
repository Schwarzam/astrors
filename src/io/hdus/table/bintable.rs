use core::panic;

use crate::io::{Header, header::card::Card, utils::pad_buffer_to_fits_block};
use polars::prelude::*; // Polars library

#[derive(Debug, PartialEq)]
pub enum Data {
    L(Vec<bool>), // Logical
    X(Vec<u8>), // Bit
    B(Vec<i8>), // Byte
    I(Vec<i16>), // Short
    J(Vec<i32>), // Int
    K(Vec<i64>), // Long
    A(Vec<String>), // Char
    E(Vec<f32>), // Float
    D(Vec<f64>), // Double
    C(Vec<String>), // Complex
    M(Vec<String>), // Double complex
    P(Vec<String>), // Array descriptor
    Q(Vec<String>), // Array descriptor
}

impl Data {
    pub fn byte_value(&self) -> usize{
        match self {
            Data::L(_) => 1,
            Data::X(_) => 1,
            Data::B(_) => 1,
            Data::I(_) => 2,
            Data::J(_) => 4,
            Data::K(_) => 8,
            Data::A(_) => 1,
            Data::E(_) => 4,
            Data::D(_) => 8,
            Data::C(_) => 8,
            Data::M(_) => 16,
            Data::P(_) => 8,
            Data::Q(_) => 16,
        }
    }

    pub fn push(&mut self, element: String, data_type: char) {
        match data_type {
            'L' => {
                let element = element.to_string().parse::<bool>().unwrap();
                match self {
                    Data::L(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'X' => {
                let element = element.to_string().parse::<u8>().unwrap();
                match self {
                    Data::X(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'B' => {
                let element = element.to_string().parse::<i8>().unwrap();
                match self {
                    Data::B(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'I' => {
                let element = element.to_string().parse::<i16>().unwrap();
                match self {
                    Data::I(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'J' => {
                let element = element.to_string().parse::<i32>().unwrap();
                match self {
                    Data::J(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'K' => {
                let element = element.to_string().parse::<i64>().unwrap();
                match self {
                    Data::K(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'A' => {
                let element = element.to_string();
                match self {
                    Data::A(data) => data.push(element),
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
            'C' => {
                let element = element.to_string();
                match self {
                    Data::C(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'M' => {
                let element = element.to_string();
                match self {
                    Data::M(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'P' => {
                let element = element.to_string();
                match self {
                    Data::P(data) => data.push(element),
                    _ => panic!("Wrong data type"),
                }
            }
            'Q' => {
                let element = element.to_string();
                match self {
                    Data::Q(data) => data.push(element),
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
            Data::L(data) => data.len(),
            Data::X(data) => data.len(),
            Data::B(data) => data.len(),
            Data::I(data) => data.len(),
            Data::J(data) => data.len(),
            Data::K(data) => data.len(),
            Data::A(data) => data.len(),
            Data::E(data) => data.len(),
            Data::D(data) => data.len(),
            Data::C(data) => data.len(),
            Data::M(data) => data.len(),
            Data::P(data) => data.len(),
            Data::Q(data) => data.len(),
        }
    }

    //no need for max_len on bintable

}

#[derive(Debug)]
pub struct Column {
    ttype: String, 
    tform: String,
    tunit: Option<String>,
    tdisp: Option<String>,
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

        if ttype.is_none() {
            break;
        }

        let ttype = ttype.unwrap().value.to_string();
        let tform = tform.unwrap().value.to_string();
        let tunit = tunit.map(|c| c.value.to_string());
        let tdisp = tdisp.map(|c| c.value.to_string());

        let tform2 = tform.clone();
        let column = Column {
            ttype,
            tform,
            tunit,
            tdisp,
            data : Data::I(Vec::new()),
            // data : match get_tform_type_size(&tform2) {
            //     ('I', _) => Data::I(Vec::new()),
            //     ('E', _) => Data::E(Vec::new()),
            //     ('D', _) => Data::D(Vec::new()),
            //     ('A', _) => Data::A(Vec::new()),
            //     ('F', _) => Data::F(Vec::new()),
            //     (_, _) => Data::A(Vec::new()),
            // }
        };

        columns.push(column);
    }

    Ok(columns)
}

pub fn columns_to_polars(columns: Vec<Column>) -> Result<DataFrame, String> {
    let mut polars_columns: Vec<Series> = Vec::new();
    for column in columns {
        let series = match column.data {
            Data::L(data) => Series::new(&column.ttype, data),
            Data::X(data) => panic!("Bit column not supported"),
            Data::B(data) => Series::new(&column.ttype, data),
            Data::I(data) => Series::new(&column.ttype, data),
            Data::J(data) => Series::new(&column.ttype, data),
            Data::K(data) => Series::new(&column.ttype, data),
            Data::A(data) => Series::new(&column.ttype, data),
            Data::E(data) => Series::new(&column.ttype, data),
            Data::D(data) => Series::new(&column.ttype, data),
            Data::C(data) => Series::new(&column.ttype, data),
            Data::M(data) => Series::new(&column.ttype, data),
            Data::P(data) => Series::new(&column.ttype, data),
            Data::Q(data) => Series::new(&column.ttype, data),
        };
        polars_columns.push(series);
    }

    let df = DataFrame::new(polars_columns).map_err(|e| e.to_string())?;
    Ok(df)
}