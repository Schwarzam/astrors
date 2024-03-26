## astrors 

[![](https://img.shields.io/crates/v/astrors.svg)](https://crates.io/crates/astrors)
[![](https://img.shields.io/crates/d/astrors.svg)](https://crates.io/crates/astrors)

![](https://github.com/schwarzam/astrors/actions/workflows/build.yml/badge.svg)
![](https://github.com/schwarzam/astrors/actions/workflows/codecov.yml/badge.svg)
[![codecov](https://codecov.io/gh/Schwarzam/astrors/graph/badge.svg?token=WFB32324PK)](https://codecov.io/gh/Schwarzam/astrors)

A package for astronomical image processing and analysis. We aim to provide a simple interface for common tasks like opening fits files, including images and tables.

### To Do list
----------

* [X] Read/Writing modifying header.
* [X] Read/Writing modifying image data.
* [-] Documentation
* [X] Read/Writing bin table data.
* [X] Keep CARD comment
* [X] Support of multiple HDU, fits extensions (in progress, only the header is parsed)
* [ ] Read / Write compressed images
* [ ] WCS operations
* [ ] General astronomy operations

----------

## Astrors Library Guide

## Introduction

This guide provides an overview of the `astrors` library, a Rust-based tool for handling FITS files used in astronomical data. With `astrors`, you can read, write, and manipulate FITS files, enabling efficient processing and analysis of astronomical datasets. This guide covers common use cases, including reading FITS files, writing data back to FITS format, and manipulating image and table data within FITS files.

## Prerequisites

Before you start, ensure you have Rust installed on your machine. You'll also need the `astrors`. This guide assumes basic familiarity with Rust programming.

## Setup

To use `astrors` in your project, add it to your `Cargo.toml` file:

```toml
[dependencies]
astrors = ""
```

## Reading FITS Files

### Basic Reading of a FITS File

To read a FITS file and access its HDUs (Header/Data Units), you can use the following approach:

```rust
use astrors::fits;

let testfile = common::get_testdata_path("your_file.fits");
let mut hdu_list = fits::fromfile(testfile.to_str().unwrap()).unwrap();

println!("HDU List Length: {:?}", hdu_list.hdus.len());
```

This code snippet opens a FITS file, reads its contents into an `HDUList` structure, and prints the number of HDUs found in the file.

## Writing FITS Files

### Writing Modified HDUs Back to a FITS File

After reading and optionally modifying HDUs, you can write them back to a new FITS file:

```rust
let outfile = common::get_outtestdata_path("modified_file.fits");
hdu_list.write_to(outfile.to_str().unwrap()).unwrap();
```

## Manipulating HDU Data

### Reading and Modifying Primary HDU

To read the primary HDU, modify its data, and write it back to a file:

```rust
use astrors::io::hdus::primaryhdu::PrimaryHDU;
use std::fs::File;
use ndarray::ArrayD;
use astrors::io::hdulist::HDUList;
use astrors::io::hdus::image::ImageData;
use ndarray::IxDyn;

let testfile = common::get_testdata_path("your_primary_hdu_file.fits");
let mut f: File = File::open(testfile)?;
let mut primary_hdu = PrimaryHDU::read_from_file(&mut f)?;

// Modify the primary HDU's data
primary_hdu.data = ImageData::F32(ArrayD::from_elem(IxDyn(&[100, 100]), 1.0));

// Write the modified primary HDU to a new file
let outfile = common::get_outtestdata_path("modified_primary_hdu.fits");
let mut f_out: File = File::create(outfile)?;
let mut hdus = HDUList::new();
hdus.add_hdu(HDU::Primary(primary_hdu));
hdus.write_to(outfile.to_str().unwrap())?;
```

### Integrating with Polars for Tabular Data

To create a binary table HDU from a `DataFrame` and add it to an `HDUList`:

```rust
use polars::prelude::*;
use astrors::io::hdus::bintable::bintablehdu::BinTableHDU;

let df = DataFrame::new(vec![
    Series::new("RA", vec![1, 2, 3, 4, 5]),
    Series::new("DEC", vec![1, 2, 3, 4, 5]),
    Series::new("MAG", vec![1, 2, 3, 4, 5]),
]).unwrap();

let mut bintable = BinTableHDU::new_data(df);
hdus.add_hdu(HDU::BinTable(bintable));
```

This snippet creates a `DataFrame` with astronomical data, converts it to a binary table HDU, and adds it to an `HDUList` for writing to a FITS file.

## Conclusion

The `astrors` library offers Rust developers powerful tools for working with astronomical data in FITS format. By reading, modifying, and writing FITS files, as well as integrating with the `polars` library for tabular data, you can efficiently handle complex astronomical datasets.