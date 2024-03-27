use crate::io::Header;

pub fn format_scientific<T>(num: T, max_len: usize) -> String 
where
    T: std::fmt::LowerExp + PartialEq + Into<f64>,
{
    let mut formatted = format!("{:.e}", num);
    
    // Replace "0e0" with "0.0" for zero representation.
    if formatted.contains("0e0") {
        formatted = formatted.replace("0e0", "0.0");
    }
    
    // Replace 'e' with 'E' for consistency in scientific notation.
    formatted = formatted.replace('e', "E");
    
    // Ensure the string does not exceed max_len.
    // If it does, truncate and ensure it does not end with "E".
    if formatted.len() > max_len {
        let mut truncated = formatted[..max_len].to_string();
        
        // If the truncated string ends with "E", remove the trailing "E".
        if truncated.ends_with('E') {
            truncated.pop(); // Remove the last character.
            
            // Optional: If you also want to ensure not to end with a ".", you can add another check here.
            if truncated.ends_with('.') {
                truncated.pop();
            }
        }
        
        truncated
    } else {
        formatted
    }
}


pub fn clear_table_on_header(header: &mut Header) {
    if header.get_card("TFIELDS").is_some(){
        let tfields = header["TFIELDS"].value.as_int().unwrap_or(0);
        for i in 1..=tfields {
            header.remove(&format!("TTYPE{}", i));
            header.remove(&format!("TFORM{}", i));
            header.remove(&format!("TUNIT{}", i));
            header.remove(&format!("TDISP{}", i));
            header.remove(&format!("TBCOL{}", i));
        }
        header.remove("PCOUNT");
        header.remove("GCOUNT");
        header.remove("BITPIX");
        header.remove("TFIELDS");
        header.remove("NAXIS");
        header.remove("NAXIS1");
        header.remove("NAXIS2");
    }
}