use std::{fs::File, io::Read};

use crate::io::{Header, header::card::Card};

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

#[derive(Debug)]
pub enum Data {
    I(Vec<i32>),
    E(Vec<f32>),
    D(Vec<f64>),
    A(Vec<String>),
    F(Vec<f32>),
}

impl Data {
    pub fn push(&mut self, element: String, data_type: char) {
        println!("Pushing element: {}", element);
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
