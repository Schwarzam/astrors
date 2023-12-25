struct HDUList {
    hdus: Vec<HDU>,
}


impl HDUList {
    pub fn new() -> Self {
        HDUList {
            hdus: Vec::new(),
        }
    }

    pub fn add_hdu(&mut self, hdu: HDU) {
        self.hdus.push(hdu);
    }

    pub fn get_hdu(&self, hdu_name: &str) -> Option<&HDU> {
        self.hdus.iter().find(|hdu| hdu.name == hdu_name)
    }

    pub fn get_mut_hdu(&mut self, hdu_name: &str) -> Option<&mut HDU> {
        self.hdus.iter_mut().find(|hdu| hdu.name == hdu_name)
    }
}

enum HDU {
    Primary(PrimaryHDU),
    Image(ImageHDU),
    Table(TableHDU),
    BinTable(BinTableHDU),
}