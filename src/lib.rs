
mod fits_io {
    use std::io;
    use std::fs::File;

    pub fn read_file() -> io::Result<()>{
        let mut f = File::open("../testdata/test.fits")?;
        
        println!("TEIIII");
        println!("f = {:?}", f);
        Ok(())
    }


}

#[test]
fn read_test() -> std::io::Result<()>{
    // crate::fits_io::read_file();

    use std::io;
    use std::fs::File;
    use std::io::prelude::*;
    let mut f = File::open("./testdata/test.fits")?;

    //I want to iterate from 0 to 10 and print the bytes
    loop {
        let mut buffer= [0; 2880];
        let n = f.read(&mut buffer[..])?;

        let stri = String::from_utf8_lossy(&buffer[..n]);
        println!("{:?}", stri);
        
        for card in stri.as_bytes().chunks(80) {
            let card_str = String::from_utf8_lossy(card).trim_end().to_string();
            
            // We are checking whether the keyword is HIERARCH.
            // If it is, we need to handle it specially.
            if card_str.starts_with("HIERARCH") {
                let splits: Vec<&str> = card_str.splitn(3, ' ').collect();
    
                // Let's check if we have at least 3 parts (HIERARCH, keyword, value).
                // If not, it's an error.
                if splits.len() < 3 {
                    // Handle error
                    continue;
                }
    
                let keyword = splits[1].to_string(); // Extracting keyword.
                let remaining = splits[2]; // The remaining string after the keyword.
    
                let (value, comment) = if let Some(idx) = remaining.find('/') {
                    // If there is a '/' character, we split the remaining string into value and comment.
                    (remaining[..idx].trim().to_string(), Some(remaining[idx+1..].trim().to_string()))
                } else {
                    // Otherwise, the whole remaining string is the value.
                    (remaining.trim().to_string(), None)
                };
    
                println!("HIERARCH keyword: {}, value: {}, comment: {:?}", keyword, value, comment);
            } else {
                // For non-HIERARCH keywords, the format is simpler.
    
                // We first check if there is a '=' character.
                // If not, it's an error.
                if let Some(idx) = card_str.find('=') {
                    let keyword = card_str[..idx].trim().to_string(); // Extracting keyword.
                    let remaining = card_str[idx+1..].trim(); // The remaining string after the '='.
    
                    let (value, comment) = if let Some(idx) = remaining.find('/') {
                        // If there is a '/' character, we split the remaining string into value and comment.
                        (remaining[..idx].trim().to_string(), Some(remaining[idx+1..].trim().to_string()))
                    } else {
                        // Otherwise, the whole remaining string is the value.
                        (remaining.trim().to_string(), None)
                    };
    
                    println!("keyword: {}, value: {}, comment: {:?}", keyword, value, comment);

                } else {
                    // Handle error
                    continue;
                }
            }
        }

        if stri.contains("END        ") {
            break;
        }
    }

    Ok(())
}
