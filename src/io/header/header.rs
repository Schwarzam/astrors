use std::fs::File;
use std::io::Read;
use std::io::{Error, ErrorKind};
use std::ops::{Index, IndexMut};

use crate::io::header::card::{Card, CardValue};
use crate::io::utils::pad_buffer_to_fits_block;

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

impl IndexMut<&str> for Header {
    fn index_mut(&mut self, card_name: &str) -> &mut Card {
        self.get_mut_card(card_name).expect("Card not found")
    }
}

impl Header {
    pub fn new() -> Self {
        Header {
            cards: Vec::new(),
        }
    }

    pub fn fix_header_w_mandatory_order(&mut self, keywords_order : &[&str]){
        self.cards.sort_by(|a, b| {
            let a_index = keywords_order.iter().position(|&k| k == a.keyword).unwrap_or(usize::MAX);
            let b_index = keywords_order.iter().position(|&k| k == b.keyword).unwrap_or(usize::MAX);
            a_index.cmp(&b_index)
        });
    }

    pub fn are_mandatory_keywords_first(&mut self, mandatory_keywords: &[&str]) -> bool {
        for (i, &keyword) in mandatory_keywords.iter().enumerate() {
            if i >= self.cards.len() || self.cards[i].keyword != keyword {
                return false;
            }
        }
        true
    }

    pub fn contains_key(&self, keyword: &str) -> bool {
        self.cards.iter().any(|card| card.keyword == keyword)
    }

    pub fn add_card(&mut self, card: &Card) {
        self.cards.push(card.clone());
    }

    pub fn get_card(&self, card_name: &str) -> Option<&Card> {
        self.cards.iter().find(|card| card.keyword == card_name)
    }

    pub fn get_mut_card(&mut self, card_name: &str) -> Option<&mut Card> {
        self.cards.iter_mut().find(|card| card.keyword == card_name)
    }

    // pub fn get_value(&self, keyword: &str) -> Result<&str, Error> {
    //     self.cards.iter().find(|card| card.keyword == keyword).map_or(
    //         Err(Error::new(ErrorKind::Other, format!("{} keyword not found", keyword))),
    //         |card| {
    //             match &card.value {
    //                 Some(value) => Ok(value.unwrap().to_string()),
    //                 None => Err(Error::new(ErrorKind::Other, format!("{} keyword has no value", keyword))),
    //             }
    //         }
    //     )
    // }

    // pub fn parse_header_value<T: std::str::FromStr>(&self, keyword: &str) -> Result<T, std::io::Error> {
    //     let value_str = self.get_value(keyword)?;
    //     value_str.parse::<T>().map_err(|_| {
    //         let err_msg = format!("Failed to parse {} as {}", keyword, std::any::type_name::<T>());
    //         std::io::Error::new(std::io::ErrorKind::Other, err_msg)
    //     })
    // }

    pub fn remove(&mut self, keyword: &str) -> Option<Card> {
        self.cards.iter().position(|card| card.keyword == keyword).map(|idx| self.cards.remove(idx))
    }

    pub fn len(&self) -> usize {
        self.cards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cards.is_empty()
    }

    pub fn clear(&mut self) {
        self.cards.clear();
    }

    pub fn iter(&self) -> std::slice::Iter<Card> {
        self.cards.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<Card> {
        self.cards.iter_mut()
    }

    pub fn pretty_print(&self) {
        for card in &self.cards {
            println!("{} = {} / {}", card.keyword, card.value.to_string(), card.comment.as_ref().unwrap_or(&String::new()));
        }
    }

    pub fn pretty_print_advanced(&self) {
        for card in &self.cards {
            println!("----------------------------------------");
            println!("Keyword: {}", card.keyword);
            println!("Value: {}", card.value.to_string());
            println!("Comment: {}", card.comment.as_ref().unwrap_or(&String::new()));
        }
    }
    
    // test
    pub fn read_from_file(&mut self, file: &mut File) -> std::io::Result<()>{
        let mut last_card : Card = Card::default();
        
        'outer: loop {
            let mut buffer= [0; 2880];
            let _ = file.read(&mut buffer[..])?;
            
            for card in buffer.chunks(80) {
                let card_str = String::from_utf8_lossy(card).trim_end().to_string();
                
                if card_str == "END" {
                    self.add_card(&last_card);
                    break 'outer;
                }
                
                if !card_str.contains("CONTINUE  ") && last_card.keyword != "" {
                    self.add_card(&last_card);
                    last_card = Card::default();
                }
                
                if card_str.contains("CONTINUE  ") {
                    if last_card.value == CardValue::EMPTY {
                        return Err(Error::new(ErrorKind::Other, "CONTINUE card without previous card"));
                    }
                    
                    let value = last_card.value.to_string();
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
        writer.write_all(end_string.as_bytes())?;  // Write the END card
        
        pad_buffer_to_fits_block(writer, bytes_count as usize)?;
        Ok(())
    }
}