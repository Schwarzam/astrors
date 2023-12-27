use std::io::{self, Read, Seek, SeekFrom};
use std::fs::File;

pub fn has_more_data(file: &mut File) -> io::Result<bool> {
    let current_pos = file.seek(SeekFrom::Current(0))?; // Save current position

    let mut buffer = [0; 1]; // Small buffer to attempt reading
    let bytes_read = file.read(&mut buffer)?; // Attempt to read

    file.seek(SeekFrom::Start(current_pos))?; // Restore original position

    Ok(bytes_read != 0) // If bytes_read is 0, we're at EOF
}