pub mod bintable;

pub mod bintablehdu;
pub mod buffer;

pub mod utils;

extern crate regex;
use regex::Regex;

pub fn get_first_letter(string : &str) -> &str {
    let re = Regex::new(r"[A-Z]").unwrap();
    if let Some(cap) = re.find(string) {
        return &string[cap.range()];
    }
    ""
}

pub fn get_data_bytes_size(string : &str) -> usize {
    let re = Regex::new(r"^(\d*)?").unwrap();
    let size = if let Some(cap) = re.find(string) {
        let number = &string[cap.range()];
        if number.is_empty() {
            1
        } else {
            number.parse::<usize>().unwrap()
        }
    }else{
        1
    };

    size * byte_value_from_str(string)
}

pub fn byte_value_from_str(data_type : &str) -> usize {
    match get_first_letter(data_type) {
        "L" => 1,
        "X" => 1,
        "B" => 1,
        "I" => 2,
        "J" => 4,
        "K" => 8,
        "A" => 1,
        "E" => 4,
        "D" => 8,
        "C" => 8,
        "M" => 16,
        "P" => 8,
        "Q" => 16,
        _ => panic!("Wrong data type"),
    }
}

pub fn split_buffer(buffer_size: usize, n: u16, row_size: u16) -> Vec<(usize, usize)> {
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

