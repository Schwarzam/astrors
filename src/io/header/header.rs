use std::fs::File;
use std::io::Read;
use std::io::{Error, ErrorKind};

use crate::io::header::card::Card;

use std::io::Write;
pub struct Header {
    cards: Vec<Card>,
}



impl Header {
    pub fn new() -> Self {
        Header {
            cards: Vec::new(),
        }
    }

    fn add_card(&mut self, card: Card) {
        self.cards.push(card);
    }

    pub fn get_card(&self, keyword: &str) -> Option<&Card> {
        for card in &self.cards {
            if card.keyword == keyword {
                return Some(card);
            }
        }

        None
    }

    pub fn get_value(&self, keyword: &str) -> Result<&str, std::io::Error> {
        for card in &self.cards {
            if card.keyword == keyword {
                return Ok(&card.value);
            }
        }
    
        Err(Error::new(ErrorKind::Other, format!("{} keyword not found", keyword).as_str()))
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
        let mut idx = None;

        for (i, card) in self.cards.iter().enumerate() {
            if card.keyword == keyword {
                idx = Some(i);
                break;
            }
        }

        if let Some(idx) = idx {
            Some(self.cards.remove(idx))
        } else {
            None
        }
    }

    fn contains(&self, keyword: &str) -> bool {
        for card in &self.cards {
            if card.keyword == keyword {
                return true;
            }
        }
        false
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
            println!("{} = {} / {}", card.keyword, card.value, card.comment.as_ref().unwrap_or(&String::new()));
        }
    }

    pub fn read_from_file(&mut self, f: &mut File) -> std::io::Result<()>{

        'outer: loop {
            let mut buffer= [0; 2880];
            let n = f.read(&mut buffer[..])?;
            
            for card in buffer.chunks(80) {
                let card_str = String::from_utf8_lossy(card).trim_end().to_string();
                if card_str == "END" {
                    break 'outer;
                }

                let card = Card::parse_card(card_str);
                println!("card: {:?}", card);

                
            } // for loop chunks 80

        } // loop over 2880 bytes buffer 

        Ok(())
    } // read_from_buffer

    pub fn write_to<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        for card in &self.cards {
            card.write_to(writer)?;
        }
        let mut end_string = "END".to_string();
        end_string.push_str(&" ".repeat(80 - end_string.len()));  // Pad the END card with spaces
        writer.write_all(end_string.as_bytes())?;  // Write the END card
        let remainder = (self.cards.len() + 1) * 80 % 2880;
        if remainder != 0 {
            let padding = " ".repeat(2880 - remainder);  // Pad to the next block
            writer.write_all(padding.as_bytes())?;  // Write the padding
        }
        Ok(())
    }

}