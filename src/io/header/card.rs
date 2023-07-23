use std::fs::File;
use std::io::Read;
use std::io::{Error, ErrorKind};

use std::io::Write;

#[derive(Debug, PartialEq)]
pub struct Card {
    pub keyword: String,
    pub value: String,
    pub comment: Option<String>,
    pub card_type: Option<TYPE>,
}

#[derive(Debug, PartialEq)]
pub enum TYPE{
    INT,
    FLOAT,
    STRING, 
    BOOL
}

fn check_type(s: &str) -> TYPE {
    if s.parse::<i32>().is_ok() {
        TYPE::INT
    } else if s.parse::<f64>().is_ok() {
        TYPE::FLOAT
    } else if s.parse::<bool>().is_ok() {
        TYPE::BOOL
    } else {
        TYPE::STRING
    }
}

impl Card {
    pub fn write_to<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
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

    pub fn parse_card(card_str: String) -> Self {
        
        let mut keyword;
        let mut value;
        let mut comment;

        if card_str.starts_with("COMMENT") || card_str.starts_with("HISTORY") || !card_str.contains("="){            
            let card = Card {
                keyword: card_str,
                value: "".to_string(),
                comment: None,
                card_type: Some( TYPE::STRING ),
            };
            return card;
        }
        if card_str.starts_with("HIERARCH"){
            keyword = card_str.splitn(2, '=').collect::<Vec<&str>>()[0].to_string();
            keyword.replace("HIERARCH ", "");
        }
        else{
            keyword = card_str.splitn(2, '=').collect::<Vec<&str>>()[0].trim().to_string();
        }

        let remaining = card_str.splitn(2, '=').collect::<Vec<&str>>()[1].trim();
        if let Some(idx) = remaining.find('/') {
            // If there is a '/' character, we split the remaining string into value and comment.
            value = remaining[..idx].trim().replace("'", "").to_string();
            comment = Some(remaining[idx+1..].trim().to_string());
        } else {
            // Otherwise, the whole remaining string is the value.
            value = remaining.trim().to_string();
            comment = None;
        };

        let card_type = check_type(&value);

        Card { keyword: keyword, value: value, comment: comment, card_type: Some( card_type ) }
    
    }
}