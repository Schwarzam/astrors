use std::fmt::format;
use std::fs::File;
use std::io::Read;
use std::io::{Error, ErrorKind};

use std::io::Write;

#[derive(Debug, PartialEq)]
pub struct Card {
    pub keyword: String,
    pub value: Option<String>,
    pub comment: Option<String>,
    pub card_type: Option<TYPE>,
}

#[derive(Debug, PartialEq)]
pub enum TYPE{
    INT,
    FLOAT,
    STRING, 
    BOOL, 
    LOGICAL
}

fn check_type(s: &str) -> TYPE {
    if s.parse::<i64>().is_ok() {
        TYPE::INT
    } else if s.parse::<f64>().is_ok() {
        TYPE::FLOAT
    } else if s.parse::<bool>().is_ok() {
        TYPE::BOOL
    } else if s == "T" || s == "F" {
        TYPE::LOGICAL
    } else {
        TYPE::STRING
    }
}

impl Card {
    fn write_formatted_string<W: Write>(&self, writer: &mut W, mut string: String, bytes_count: &mut i32) -> std::io::Result<()> {
        string.truncate(80);
        string.push_str(&" ".repeat(80 - string.len()));
        *bytes_count += 80;
        writer.write_all(string.as_bytes())
    }
    
    pub fn write_to<W: Write>(&self, writer: &mut W, bytes_count: &mut i32) -> std::io::Result<()> {
        if self.keyword == "COMMENT" || self.keyword == "HISTORY" || self.value.is_none() {
            self.write_formatted_string(writer, format!("{:<80}", self.keyword), bytes_count)
        } else {
            let keyword_string = if self.keyword.len() > 8 {
                format!("HIERARCH {:} = ", self.keyword)
            } else {
                format!("{:8}= ", self.keyword)
            };
    
            match self.card_type {
                Some(TYPE::STRING) => self.write_string_card(writer, keyword_string, bytes_count),
                _ => self.write_other_card(writer, keyword_string, bytes_count),
            }
        }
    }
    
    fn write_string_card<W: Write>(&self, writer: &mut W, keyword_string: String, bytes_count: &mut i32) -> std::io::Result<()> {
        let mut formatted_value = self.value.clone().unwrap();
        let remaining_value = if formatted_value.len() > 67 {
            let remainder = Some(formatted_value[68..].to_string());
            formatted_value.truncate(68);
            remainder
        } else {
            None
        };
    
        let mut card_string = format!("{}'{}'", keyword_string, formatted_value);
        if let Some(comment) = &self.comment {
            card_string = format!("{} / {}", card_string, comment);
        }
        self.write_formatted_string(writer, card_string, bytes_count)?;
    
        if let Some(mut remaining_value) = remaining_value {
            while !remaining_value.is_empty() {
                let len = remaining_value.len();
                let take = len.min(68);
                let continue_card = format!("CONTINUE  '{}'", &remaining_value[..take]);
                self.write_formatted_string(writer, continue_card, bytes_count)?;
    
                remaining_value.drain(..take);
            }
        }
        Ok(())
    }
    
    fn write_other_card<W: Write>(&self, writer: &mut W, keyword_string: String, bytes_count: &mut i32) -> std::io::Result<()> {
        // using unwrap_or with an empty string as default
        let formatted_value = format!("{:>20}", self.value.as_ref().unwrap_or(&"".to_string()));
        let mut card_string = format!("{}{}", keyword_string, formatted_value);
        if let Some(comment) = &self.comment {
            card_string = format!("{} / {}", card_string, comment);
        }
        self.write_formatted_string(writer, card_string, bytes_count)
    }

    pub fn parse_card(card_str: String) -> Option<Self> {
        if card_str.trim().len() < 1 {
            return None;
        }

        let mut keyword;
        let mut value;
        let mut comment;

        if card_str.starts_with("COMMENT") || card_str.starts_with("HISTORY") || !card_str.contains("="){            
            let card = Card {
                keyword: card_str,
                value: None,
                comment: None,
                card_type: Some( TYPE::STRING ),
            };
            return Some(card);
        }
        if card_str.starts_with("HIERARCH"){
            keyword = card_str.splitn(2, '=').collect::<Vec<&str>>()[0].to_string();
            keyword = keyword.replace("HIERARCH ", "");
        }
        else{
            keyword = card_str.splitn(2, '=').collect::<Vec<&str>>()[0].trim().to_string();
        }

        let remaining = card_str.splitn(2, '=').collect::<Vec<&str>>()[1].trim();
        if let Some(idx) = remaining.find(" / ") {
            // If there is a '/' character, we split the remaining string into value and comment.
            value = remaining[..idx + 1].trim().replace("'", "").to_string();
            comment = Some(remaining[idx+2..].trim().to_string());
        } else {
            // Otherwise, the whole remaining string is the value.
            value = remaining.trim().replace("'", "").to_string();
            comment = None;
        };

        let card_type = check_type(&value);

        Some(
            Card { keyword: keyword, value: Some( value ), comment: comment, card_type: Some( card_type ) }
        )
    }

    pub fn continue_card(card: &mut Option<Card>, card_str: String){
        if let Some(card) = card {
            let value;
            if card_str.starts_with("CONTINUE  "){
                value = card_str.splitn(2, "CONTINUE  ").collect::<Vec<&str>>()[1].trim().replace("'", "").to_string();
                card.value = Some(format!("{}{}", card.value.as_ref().unwrap_or(&"".to_string()), value));
            }
        }
    }

}