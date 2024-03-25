pub mod bintable;

pub mod bintablehdu;
pub mod buffer;

pub fn get_tform_type_size(tform: &str) -> (char, usize) {
    let tform = tform.trim();
    
    //return the last char of tform
    let tform_type = tform.chars().last().unwrap_or('A');
    let mut size = byte_value_from_str(&tform_type.to_string());
    if tform_type == 'A' {
        // The number is before the A like 48A or 8A
        size = tform[0..tform.len()-1].parse::<usize>().unwrap_or(0);
    }

    (tform_type, size)
}

pub fn byte_value_from_str(data_type : &str) -> usize {
    match data_type {
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