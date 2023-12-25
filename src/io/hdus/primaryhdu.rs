use crate::io::Header;
use crate::io::hdus::image::ImageData;

struct PrimaryHDU{
    header: Header,
    data: ImageData,
}

