
use astrors::io::hdus::bintable::bintable::read_tableinfo_from_header;
use astrors::fits;

use std::io::Result;

mod common;

#[test]
fn test_read_fits() -> Result<()> {

    let file = common::get_testdata_path("0.1_0.1_300_R_swp_splus.fits.fz");
    let fits = fits::fromfile(file.to_str().unwrap())?;
    println!("{:?}", fits);

    // read_tableinfo_from_header()
    Ok(())
}