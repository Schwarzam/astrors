use std::io::Write;

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