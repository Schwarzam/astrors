use std::fs::File;
use std::io::Read;
use std::io::{Error, ErrorKind};

use std::io::Write;
pub struct Header {
    cards: Vec<Card>,
}

pub struct Card {
    pub keyword: String,
    pub value: String,
    pub comment: Option<String>,
}

impl Card {
    fn write_to<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let card_string = if self.keyword == "COMMENT" || self.keyword == "HISTORY" || self.value == "" {
            format!("{:<80}", self.keyword)
        } else if self.keyword.starts_with("HIERARCH") {
            let formatted_value = if self.value.contains("'") {
                format!("{:}", self.value)
            } else {
                format!("{:>20}", self.value)
            };

            let mut card_string = format!("{}= {} /", self.keyword, formatted_value);
            if let Some(ref comment) = self.comment {
                card_string = format!("{} {}", card_string, comment);
            }
            card_string.truncate(80);  // Ensure it's 80 bytes
            card_string.push_str(&" ".repeat(80 - card_string.len()));  // Pad with spaces if needed
            card_string
        } else {
            let formatted_value = if self.value.contains("'") {
                format!("{:}", self.value)
            } else {
                format!("{:>20}", self.value)
            };

            let mut card_string = format!("{:8}= {} /", self.keyword, formatted_value);
            if let Some(ref comment) = self.comment {
                card_string = format!("{} {}", card_string, comment);
            }
            card_string.truncate(80);  // Ensure it's 80 bytes
            card_string.push_str(&" ".repeat(80 - card_string.len()));  // Pad with spaces if needed
            card_string
        };
        println!("card_string: {}", card_string.as_bytes().len());
        writer.write_all(card_string.as_bytes())  // Write to the writer
    }
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
                println!("card_str: {}", card_str);
                // We are checking whether the keyword is HIERARCH.
                // If it is, we need to handle it specially.
                if card_str == "END" {
                    break 'outer;
                }

                else if card_str.starts_with("COMMENT") || card_str.starts_with("HISTORY") || !card_str.contains("="){
                    
                    let card = Card {
                        keyword: card_str,
                        value: "".to_string(),
                        comment: None,
                    };
                    self.add_card(card);
                }

                else if card_str.starts_with("HIERARCH") {
                    let splits: Vec<&str> = card_str.splitn(2, '=').collect();
                    
                    
                    // Let's check if we have at least 3 parts (HIERARCH, keyword, value).
                    // If not, it's an error.
                    if splits.len() < 2 {
                        // Handle error
                        continue;
                    }
                    
                    let keyword = splits[0].to_string(); // Extracting keyword.
                    let remaining = splits[1]; // The remaining string after the keyword.
        
                    let (value, comment) = if let Some(idx) = remaining.find('/') {
                        // If there is a '/' character, we split the remaining string into value and comment.
                        (remaining[..idx].trim().to_string(), Some(remaining[idx+1..].trim().to_string()))
                    } else {
                        // Otherwise, the whole remaining string is the value.
                        (remaining.trim().to_string(), None)
                    };
        
                    println!("HIERARCH keyword: {}, value: {}, comment: {:?}", keyword, value, comment);

                    let card = Card {
                        keyword: keyword,
                        value: value,
                        comment: comment,
                    };
                    self.add_card(card);

                } else {
                    // For non-HIERARCH keywords, the format is simpler.
        
                    // We first check if there is a '=' character.
                    // If not, it's an error.
                    if let Some(idx) = card_str.find('=') {
                        let keyword = card_str[..idx].trim().to_string(); // Extracting keyword.
                        let remaining = card_str[idx+1..].trim(); // The remaining string after the '='.
        
                        let (value, comment) = if let Some(idx) = remaining.find('/') {
                            // If there is a '/' character, we split the remaining string into value and comment.
                            (remaining[..idx].trim().to_string(), Some(remaining[idx+1..].trim().to_string()))
                        } else {
                            // Otherwise, the whole remaining string is the value.
                            (remaining.trim().to_string(), None)
                        };
        
                        println!("keyword: {}, value: {}, comment: {:?}", keyword, value, comment);
                        let card = Card {
                            keyword: keyword,
                            value: value,
                            comment: comment,
                        };
                        self.add_card(card);

                    } else {
                        // Handle error
                        continue;
                    }

                }

                
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