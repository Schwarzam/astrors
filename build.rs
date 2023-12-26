fn main() {
    
    println!("cargo:rustc-link-search=lib/fitsio2");

    cc::Build::new()
        .file("src/cextern/fits_hcompress.c")
        .file("src/cextern/fits_hdecompress.c")
        .include("src/cextern/fitsio2.h")
        .compile("cfitsio");
}