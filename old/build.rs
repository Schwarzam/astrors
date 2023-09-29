fn main() {
    
    println!("cargo:rustc-link-search=lib/fitsio2");

    cc::Build::new()
        .file("lib/fits_hcompress.c")
        .file("lib/fits_hdecompress.c")
        .include("lib/fitsio2.h")
        .compile("cfitsio");
}