
struct HDUList {
    hdus: Vec<HDU>,
}

impl HDUList {
    pub fn new() -> HDUList {
        HDUList {
            hdus: Vec::new(),
        }
    }

    pub fn add_hdu(&mut self, hdu: HDU) {
        self.hdus.push(hdu);
    }

    pub fn write_to(&self, mut writer: impl std::io::Write) -> std::io::Result<()> {
        for hdu in &self.hdus {
            hdu.write_to_buffer(&mut writer)?;
        }
        Ok(())
    }
}

struct hdu {
    header: Header,
    data: Data,
}

enum HDU {
    PrimaryHDU(hdu),
    ImageHDU(hdu),
    TableHDU(hdu),
    BinTableHDU(hdu)
}