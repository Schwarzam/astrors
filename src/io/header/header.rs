use std::fs::File;
use std::io::Read;
use std::io::{Error, ErrorKind};
use std::ops::Index;

use crate::io::header::card::Card;

use std::io::Write;
pub struct Header {
    cards: Vec<Card>,
}

impl Index<&str> for Header {
    type Output = Card;

    fn index(&self, card_name: &str) -> &Self::Output {
        if let Some(card) = self.get_card(card_name){
            return card;
        }{
            panic!("Card {} not found", card_name);
        }
    }

}

impl Header {
    pub fn new() -> Self {
        Header {
            cards: Vec::new(),
        }
    }

    fn add_card(&mut self, card: Option<Card>) {
        if let Some(card) = card {
            self.cards.push(card);
        }
    }

    pub fn get_card(&self, card_name: &str) -> Option<&Card> {
        self.cards.iter().find(|&card| card.keyword == card_name)
    }

    pub fn get_value(&self, keyword: &str) -> Result<&str, Error> {
        self.cards.iter().find(|card| card.keyword == keyword).map_or(
            Err(Error::new(ErrorKind::Other, format!("{} keyword not found", keyword))),
            |card| {
                match &card.value {
                    Some(value) => Ok(value),
                    None => Err(Error::new(ErrorKind::Other, format!("{} keyword has no value", keyword))),
                }
            }
        )
    }

    pub fn parse_header_value<T: std::str::FromStr>(&self, keyword: &str) -> Result<T, std::io::Error> {
        let value_str = self.get_value(keyword)?;
        value_str.parse::<T>().map_err(|_| {
            let err_msg = format!("Failed to parse {} as {}", keyword, std::any::type_name::<T>());
            std::io::Error::new(std::io::ErrorKind::Other, err_msg)
        })
    }

    fn get_card_mut(&mut self, keyword: &str) -> Option<&mut Card> {
        for card in &mut self.cards {
            if card.keyword == keyword {
                return Some(card);
            }
        }

        None
    }

    fn remove_card(&mut self, keyword: &str) -> Option<Card> {
        self.cards.iter().position(|card| card.keyword == keyword).map(|idx| self.cards.remove(idx))
    }

    fn contains(&self, keyword: &str) -> bool {
        self.cards.iter().any(|card| card.keyword == keyword)
    }

    fn len(&self) -> usize {
        self.cards.len()
    }

    fn is_empty(&self) -> bool {
        self.cards.is_empty()
    }

    fn clear(&mut self) {
        self.cards.clear();
    }

    fn iter(&self) -> std::slice::Iter<Card> {
        self.cards.iter()
    }

    fn iter_mut(&mut self) -> std::slice::IterMut<Card> {
        self.cards.iter_mut()
    }

    pub fn pretty_print(&self) {
        for card in &self.cards {
            println!("{} = {} / {}", card.keyword, card.value.as_ref().unwrap_or(&String::new()), card.comment.as_ref().unwrap_or(&String::new()));
        }
    }

    pub fn read_from_file(&mut self, file: &mut File) -> std::io::Result<()>{

        'outer: loop {
            let mut buffer= [0; 2880];
            let n = file.read(&mut buffer[..])?;
            let mut last_card : Option<Card> = None;

            for card in buffer.chunks(80) {
                let card_str = String::from_utf8_lossy(card).trim_end().to_string();

                if card_str == "END" {
                    self.add_card(last_card);
                    break 'outer;
                }

                if last_card.is_some() && !card_str.contains("CONTINUE  ") {
                    self.add_card(last_card);
                    last_card = None;
                }
                
                if card_str.contains("CONTINUE  ") {
                    if last_card.is_none() {
                        return Err(Error::new(ErrorKind::Other, "CONTINUE card without previous card"));
                    }
                    
                    let value = last_card.as_ref().unwrap().value_ref();
                    if value.trim().ends_with("&"){
                        Card::continue_card(&mut last_card, card_str);
                    }
                    else{
                        return Err(Error::new(ErrorKind::Other, "CONTINUE card without previous card"))
                    }
                }
                else {
                    last_card = Card::parse_card(card_str);
                }
            } // for loop chunks 80

        } // loop over 2880 bytes buffer 

        Ok(())
    } // read_from_buffer

    pub fn write_to_buffer<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let mut bytes_count = 0;
        for card in &self.cards {
            card.write_to(writer, &mut bytes_count)?;
        }
        let mut end_string = "END".to_string();
        end_string.push_str(&" ".repeat(80 - end_string.len()));  // Pad the END card with spaces
        bytes_count += 80;

        println!("bytes_count: {}", bytes_count);
        writer.write_all(end_string.as_bytes())?;  // Write the END card
        let remainder = bytes_count as usize % 2880;
        if remainder != 0 {
            let padding = " ".repeat(2880 - remainder);  // Pad to the next block
            writer.write_all(padding.as_bytes())?;  // Write the padding
        }
        Ok(())
    }

    

    fn pad_to_fits_block<W: Write>(writer: &mut W, current_size: usize) -> std::io::Result<()> {
        const FITS_BLOCK_SIZE: usize = 2880;
        let remainder = current_size % FITS_BLOCK_SIZE;
        if remainder > 0 {
            let padding = FITS_BLOCK_SIZE - remainder;
            writer.write_all(&vec![b' '; padding])
        } else {
            Ok(())
        }
    }

}