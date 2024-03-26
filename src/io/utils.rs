use std::{fs::File, io::{Seek, Write}};

/// Writes the remainder of to reach `FITS_BLOCK_SIZE`.
pub fn pad_buffer_to_fits_block<W: Write>(writer: &mut W, current_size: usize) -> std::io::Result<()> {
    const FITS_BLOCK_SIZE: usize = 2880;
    let remainder = current_size % FITS_BLOCK_SIZE;
    if remainder > 0 {
        let padding = FITS_BLOCK_SIZE - remainder;
        writer.write_all(&vec![b' '; padding])
    } else {
        Ok(())
    }
}

/// Reads the remainder of to reach `FITS_BLOCK_SIZE`.
pub fn pad_read_buffer_to_fits_block(file: &mut File, current_size: usize) -> std::io::Result<()> {
    const FITS_BLOCK_SIZE: usize = 2880;
    let remainder = current_size % FITS_BLOCK_SIZE;
    if remainder > 0 {
        let padding = FITS_BLOCK_SIZE - remainder;
        file.seek(std::io::SeekFrom::Current(padding as i64))?;
        Ok(())
    } else {
        Ok(())
    }
}