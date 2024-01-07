use crate::io::{Header, header::card::Card};

fn parse_tform(tform: &str) -> Result<(char, usize), String> {
    if tform.is_empty() {
        return Err("TFORM string is empty".to_string());
    }

    let data_type = tform.chars().last().ok_or("Invalid TFORM string")?;
    let size_str = &tform[..tform.len() - 1];
    let size = size_str.parse::<usize>().map_err(|_| "Invalid size in TFORM")?;

    Ok((data_type, size))
}

#[test]
fn test_parse_tform() {
    assert_eq!(parse_tform("1E"), Ok(('E', 1)));
    assert_eq!(parse_tform("1D"), Ok(('D', 1)));
    assert_eq!(parse_tform("1J"), Ok(('J', 1)));
    assert_eq!(parse_tform("1K"), Ok(('K', 1)));
    assert_eq!(parse_tform("1L"), Ok(('L', 1)));
    assert_eq!(parse_tform("1A"), Ok(('A', 1)));
    assert_eq!(parse_tform("1B"), Ok(('B', 1)));
    assert_eq!(parse_tform("1C"), Ok(('C', 1)));
    assert_eq!(parse_tform("1I"), Ok(('I', 1)));
    assert_eq!(parse_tform("1P"), Ok(('P', 1)));
    assert_eq!(parse_tform("1Q"), Ok(('Q', 1)));
    

    println!("{:?}", parse_tform("E15.7"));
}

#[derive(Debug)]
struct Column {
    ttype: String, 
    tform: String,
    tunit: Option<String>,
    tdisp: Option<String>,
    tbcol: Option<i32>,
}

fn read_tableinfo_from_header(header: &Header) -> Result<(), String> {
    let mut columns: Vec<Column> = Vec::new();
    let tfields = header["TFIELDS"].value.as_int().unwrap_or(0);

    for i in 1..=tfields {
        let ttype = header.get_card(&format!("TTYPE{}", i));
        let tform = header.get_card(&format!("TFORM{}", i));
        let tunit = header.get_card(&format!("TUNIT{}", i));
        let tdisp = header.get_card(&format!("TDISP{}", i));
        let tbcol = header.get_card(&format!("TBCOL{}", i));

        if ttype.is_none() {
            break;
        }

        let ttype = ttype.unwrap().value.to_string();
        let tform = tform.unwrap().value.to_string();
        let tunit = tunit.map(|c| c.value.to_string());
        let tdisp = tdisp.map(|c| c.value.to_string());
        let tbcol = tbcol.map(|c| c.value.to_string().parse::<i32>().unwrap());


        let column = Column {
            ttype,
            tform,
            tunit,
            tdisp,
            tbcol,
        };

        columns.push(column);
    }

    println!("Columns: {:?}", columns);
    Ok(())
}

#[test]
fn test_read_table_from_header() {
    let mut header = Header::new();
    header.add_card(Card::new("XTENSION".to_string(), "BINTABLE".to_string(), None));
    header.add_card(Card::new("BITPIX".to_string(), "8".to_string(), None));
    header.add_card(Card::new("NAXIS".to_string(), "2".to_string(), None));
    header.add_card(Card::new("NAXIS1".to_string(), "8".to_string(), None));
    header.add_card(Card::new("NAXIS2".to_string(), "8".to_string(), None));
    header.add_card(Card::new("PCOUNT".to_string(), "0".to_string(), None));
    header.add_card(Card::new("GCOUNT".to_string(), "1".to_string(), None));
    header.add_card(Card::new("TFIELDS".to_string(), "2".to_string(), None));
    header.add_card(Card::new("TTYPE1".to_string(), "X".to_string(), None));
    header.add_card(Card::new("TFORM1".to_string(), "1E".to_string(), None));
    header.add_card(Card::new("TTYPE2".to_string(), "Y".to_string(), None));
    header.add_card(Card::new("TFORM2".to_string(), "1E".to_string(), None));

    // use crate::io::hdus::primaryhdu::PrimaryHDU;

    // let f = std::fs::File::open("/home/alex/Downloads/2mass-atlas.fits").unwrap();
    // let byte_pos = PrimaryHDU::get_end_byte_position(f);

    read_tableinfo_from_header(&header).unwrap();
}