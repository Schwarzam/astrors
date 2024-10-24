use crate::io::hdulist::HDUList;
use crate::io::hdulist::HDU;

use super::hdus::image::ImageData;

/// Returns data from the first non-empty imagehdu
pub fn get_data(filename: &str) -> Option<ImageData> {
    let hdulist = HDUList::fromfile(filename).unwrap();

    for hdu in hdulist.hdus {
        match hdu {
            HDU::Primary(hdu) => {
                if !hdu.data.is_empty() {
                    return Some(hdu.data);
                }
            },
            HDU::Image(hdu) => {
                if !hdu.data.is_empty() {
                    return Some(hdu.data);
                }
            },
            _ => {}
        }
    }

    None
}