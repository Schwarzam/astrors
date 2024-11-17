pub mod bintable;

pub mod bintablehdu;
pub mod buffer;

pub mod utils;

extern crate regex;
use regex::Regex;

/// Extracts the first uppercase letter from a string.
///
/// # Arguments
/// - `string` (&str): The input string.
///
/// # Returns
/// - `&str`: The first uppercase letter found in the string, or an empty string if none exists.
///
pub fn get_first_letter(string : &str) -> &str {
    let re = Regex::new(r"[A-Z]").unwrap();
    if let Some(cap) = re.find(string) {
        return &string[cap.range()];
    }
    ""
}

/// Calculates the total byte size of a data type based on its format string.
///
/// # Arguments
/// - `string` (&str): The format string (e.g., "10A", "2E").
///
/// # Returns
/// - `usize`: The total byte size for the data type.
///
/// # Behavior
/// - Parses the numeric prefix to determine the number of elements.
/// - Multiplies the element count by the byte size of the data type (determined by `byte_value_from_str`).
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

/// Determines the byte size of a single element based on its data type format string.
///
/// # Arguments
/// - `data_type` (&str): The format string representing the data type.
///
/// # Returns
/// - `usize`: The byte size of a single element of the specified data type.
///
/// # Supported Data Types
/// - `"L"`: Logical (1 byte)
/// - `"X"`: Bit (1 byte)
/// - `"B"`: Byte (1 byte)
/// - `"I"`: Short integer (2 bytes)
/// - `"J"`: Integer (4 bytes)
/// - `"K"`: Long integer (8 bytes)
/// - `"A"`: ASCII character (1 byte)
/// - `"E"`: Float (4 bytes)
/// - `"D"`: Double (8 bytes)
/// - `"C"`: Complex (8 bytes)
/// - `"M"`: Double complex (16 bytes)
/// - `"P"`: Array descriptor (8 bytes)
/// - `"Q"`: Array descriptor (16 bytes)
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

/// Splits a buffer into evenly distributed chunks, aligned by row size.
///
/// # Arguments
/// - `buffer_size` (usize): The total size of the buffer (in bytes).
/// - `n` (u16): The number of chunks to divide the buffer into.
/// - `row_size` (u16): The size of a single row (in bytes).
///
/// # Returns
/// - `Vec<(usize, usize)>`: A vector of `(start, end)` index pairs representing the buffer splits.
///
/// # Behavior
/// - Ensures that each chunk aligns with the row size.
/// - The last chunk may be larger to include any remaining bytes.
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

