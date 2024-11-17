use std::io::{self, Read, Seek, SeekFrom};
use std::fs::File;

/// Checks if there is more data to read in the file.
///
/// # Arguments
/// - `file` (&mut File): A mutable reference to the file to check.
///
/// # Returns
/// - `io::Result<bool>`: 
///   - `Ok(true)` if there is more data to read.
///   - `Ok(false)` if the end of the file (EOF) is reached.
///   - `Err(io::Error)` if an I/O error occurs.
///
/// # Behavior
/// - Saves the current position of the file cursor.
/// - Attempts to read a single byte to determine if there is more data.
/// - Restores the file cursor to its original position.
pub fn buffer_has_more_data(file: &mut File) -> io::Result<bool> {
    let current_pos = file.stream_position()?; // Save current position

    let mut buffer = [0; 1]; // Small buffer to attempt reading
    let bytes_read = file.read(&mut buffer)?; // Attempt to read

    file.seek(SeekFrom::Start(current_pos))?; // Restore original position

    Ok(bytes_read != 0) // If bytes_read is 0, we're at EOF
}