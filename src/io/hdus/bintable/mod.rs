pub mod bintable;

pub mod bintablehdu;
pub mod buffer;

pub mod utils;
pub mod listtype;

extern crate regex;
use regex::Regex;

fn get_tform_type_size(tform: &str) -> (String, usize) {
    let tform = tform.trim();
    
    // capture array descriptor columns
    // let re = Regex::new(r"1([PQ][A-Z])\((\d+)\)").unwrap();
    // match re.captures(tform) {
    //     Some(caps) => {
    //         let tform_type =  &caps[1]; // Shows the matched two-letter sequence
    //         let size = &caps[2].parse::<usize>().unwrap(); // Shows the number inside the parentheses
            
    //         println!("tform_type: {}, size: {}", tform_type, size);
    //         return (tform_type.to_string(), size.to_owned())
    //     },
    //     _ => {},
    // }

    let re = Regex::new(r"(\d+)?([A-Za-z]+)([^\\]*)$").unwrap();
    match re.captures(tform) {
        Some(caps) => {
            let tform_type = caps[2].to_string(); // Always capture the letter
            let size = if tform_type == "A" {
                caps.get(1).map_or(None, |m| m.as_str().parse::<usize>().ok()).unwrap()
            } else {
                byte_value_from_str(&tform_type)
            };
            return (tform_type, size)
        },
        _ => {},
    }
    panic!("No match found.");
}

pub fn byte_value_from_str(data_type : &str) -> usize {
    match data_type.chars().next().unwrap() {
        'L' => 1,
        'X' => 1,
        'B' => 1,
        'I' => 2,
        'J' => 4,
        'K' => 8,
        'A' => 1,
        'E' => 4,
        'D' => 8,
        'C' => 8,
        'M' => 16,
        'P' => 8,
        'Q' => 16,
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

