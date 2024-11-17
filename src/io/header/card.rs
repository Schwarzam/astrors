use std::io::Write;

/// Represents a header card in a FITS file.
#[derive(Debug, PartialEq, Clone)]
pub struct Card {
    pub keyword: String,
    pub value: CardValue,
    pub comment: Option<String>,
}

/// Represents the possible data types for a card value.
#[derive(Debug, PartialEq, Clone)]
pub enum CardValue{
    INT(i64),
    FLOAT(f64),
    STRING(String), 
    LOGICAL(bool),
    EMPTY,
}

impl CardValue {
    /// Returns the value as an integer if applicable.
    pub fn as_int(&self) -> Option<i64> {
        if let CardValue::INT(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    /// Returns the value as a float if applicable.
    pub fn as_float(&self) -> Option<f64> {
        if let CardValue::FLOAT(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    /// Returns the value as a boolean if applicable.
    pub fn as_bool(&self) -> Option<bool> {
        if let CardValue::LOGICAL(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    /// Converts the value to its string representation.
    pub fn to_string(&self) -> String {
        match self {
            CardValue::INT(value) => value.to_string(),
            CardValue::FLOAT(value) => value.to_string(),
            CardValue::STRING(value) => value.clone(),
            CardValue::LOGICAL(value) => value.to_string(),
            CardValue::EMPTY => "".to_string(),
        }
    }


}

fn check_type(s: &str) -> CardValue {
    if s.parse::<i64>().is_ok() {
        CardValue::INT(s.parse::<i64>().unwrap())
    } else if s.parse::<f64>().is_ok() {
        CardValue::FLOAT(s.parse::<f64>().unwrap())
    } else if s == "T" || s == "F" || s.parse::<bool>().is_ok() {
        if s == "T" {
            CardValue::LOGICAL(true)
        } else if s == "F" {
            CardValue::LOGICAL(false)
        } else {
            CardValue::LOGICAL(s.parse::<bool>().unwrap())
        }
    } else {
        CardValue::STRING(s.to_string())
    }
}

impl Default for Card {
    /// Provides a default `Card` with empty values.
    fn default() -> Self {
        Card {
            keyword: "".to_string(),
            value: CardValue::EMPTY,
            comment: None,
        }
    }
}

impl Card {
    /// Creates a new `Card`.
    ///
    /// # Arguments
    /// - `keyword`: The card keyword.
    /// - `value`: The card value as a string.
    /// - `comment`: An optional comment.
    pub fn new(keyword: String, value: String, comment: Option<String>) -> Self {
        Card {
            keyword: keyword,
            value: check_type(&value),  // Assuming value is always a string
            comment: comment,
        }
    }

    /// Sets the card's value.
    ///
    /// # Arguments
    /// - `new_value`: The new value to set.
    pub fn set_value(&mut self, new_value: String) {
        self.value = check_type(&new_value);
    }

    /// Retrieves the card's value as a cloned string.
    pub fn get_value_clone(&self) -> String {
        self.value.to_string()
    }

    /// Sets the card's comment.
    ///
    /// # Arguments
    /// - `new_comment`: The new comment to set.
    pub fn set_comment(&mut self, new_comment: String) {
        self.comment = Some(new_comment);
    }

    /// Retrieves the card's comment as a cloned string.
    pub fn get_comment_clone(&self) -> String {
        self.comment.clone().unwrap_or("".to_string())
    }

    /// Sets the card's keyword.
    ///
    /// # Arguments
    /// - `new_keyword`: The new keyword to set.
    pub fn set_keyword(&mut self, new_keyword: String) {
        self.keyword = new_keyword;
    }

    /// Retrieves the card's keyword as a cloned string.
    pub fn get_keyword_clone(&self) -> String {
        self.keyword.clone()
    }

    /// Writes a formatted string to a writer. The string is truncated to 80 characters and padded with spaces.
    fn write_formatted_string<W: Write>(&self, writer: &mut W, mut string: String, bytes_count: &mut i32) -> std::io::Result<()> {
        string.truncate(80);
        string.push_str(&" ".repeat(80 - string.len()));
        *bytes_count += 80;
        writer.write_all(string.as_bytes())
    }

    /// Writes the card to a writer in the FITS format.
    ///
    /// # Arguments
    /// - `writer`: The writer to write the card to.
    /// - `bytes_count`: A counter to track the number of bytes written.
    pub fn write_to<W: Write>(&self, writer: &mut W, bytes_count: &mut i32) -> std::io::Result<()> {
        if self.keyword == "COMMENT" || self.keyword == "HISTORY" || self.value == CardValue::EMPTY {
            self.write_formatted_string(writer, format!("{:<80}", self.keyword), bytes_count)
        } else {
            let keyword_string = if self.keyword.len() > 8 {
                format!("HIERARCH {:} = ", self.keyword)
            } else {
                format!("{:8}= ", self.keyword)
            };
    
            match self.value {
                CardValue::STRING(_) => self.write_string_card(writer, keyword_string, bytes_count),
                _ => self.write_other_card(writer, keyword_string, bytes_count),
            }
        }
    }

    /// Retrieves the card's keyword as a reference.
    pub fn keyword_ref(&self) -> &str {
        self.keyword.as_ref()
    }

    /// Retrieves the card's value as a reference.
    pub fn comment_ref(&self) -> &str {
        self.comment.as_ref().unwrap()
    }

    /// Writes a string card to a writer in the FITS format.
    ///
    /// # Arguments
    /// - `writer`: The writer to write the card to.
    /// - `keyword_string`: The formatted keyword string.
    /// - `bytes_count`: A counter to track the number of bytes written.
    fn write_string_card<W: Write>(&self, writer: &mut W, keyword_string: String, bytes_count: &mut i32) -> std::io::Result<()> {
        if self.keyword == "" {
            return Ok(());
        }
        
        let mut formatted_value = self.value.to_string();
        let remaining_value = if formatted_value.len() > 67 {
            let remainder = Some(formatted_value[67..].to_string());
            formatted_value.truncate(67);
            formatted_value.push_str("&");
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
                let take = len.min(67);
                let continue_card = format!("CONTINUE  '{}&'", &remaining_value[..take]);
                self.write_formatted_string(writer, continue_card, bytes_count)?;
    
                remaining_value.drain(..take);
            }
        }
        Ok(())
    }
    

    fn write_other_card<W: Write>(&self, writer: &mut W, keyword_string: String, bytes_count: &mut i32) -> std::io::Result<()> {
        // using unwrap_or with an empty string as default
        if self.keyword == "" {
            return Ok(());
        }

        let formatted_value;

        match self.value {
            CardValue::LOGICAL(_) => {
                if self.value.as_bool().unwrap() {
                    formatted_value = format!("{:>20}", "T".to_string());
                }
                else {
                    formatted_value = format!("{:>20}", "F".to_string());
                }
            },
            _ => formatted_value = format!("{:>20}", self.value.to_string()),
        }

        let mut card_string = format!("{}{}", keyword_string, formatted_value);
        if let Some(comment) = &self.comment {
            card_string = format!("{} / {}", card_string, comment);
        }
        self.write_formatted_string(writer, card_string, bytes_count)
    }
    
    pub fn parse_card(card_str: String) -> Self {
        if card_str.trim().len() < 1 {
            return Card::default();
        }

        let mut keyword;
        let value;
        let comment;

        if card_str.starts_with("COMMENT") || card_str.starts_with("HISTORY") || !card_str.contains("="){            
            let card = Card {
                keyword: card_str,
                value: CardValue::EMPTY,
                comment: None
            };
            return card;
        }
        if card_str.starts_with("HIERARCH"){
            keyword = card_str.splitn(2, '=').collect::<Vec<&str>>()[0].to_string();
            keyword = keyword.replace("HIERARCH ", "");
        }
        else{
            keyword = card_str.splitn(2, '=').collect::<Vec<&str>>()[0].trim().to_string();
        }

        keyword = keyword.trim_end().to_string();

        let remaining = card_str.splitn(2, '=').collect::<Vec<&str>>()[1].trim();
        if let Some(idx) = remaining.find(" /") {
            // If there is a '/' character, we split the remaining string into value and comment.
            value = remaining[..idx + 1].trim().replace("'", "").to_string();
            comment = Some(remaining[idx+2..].trim().to_string());
        } else {
            // Otherwise, the whole remaining string is the value.
            value = remaining.trim().replace("'", "").to_string();
            comment = None;
        };
        // println!("{} {} {:?} {:?}", keyword, value, comment, card_type);

        Card { keyword: keyword, value: check_type(&value), comment: comment }
    }

    pub fn continue_card(card: &mut Card, card_str: String){
        let mut value;
        if card_str.starts_with("CONTINUE  "){
            value = card_str.splitn(2, "CONTINUE  ").collect::<Vec<&str>>()[1].trim().replace("'", "").to_string();
            value = value.strip_suffix("&").unwrap_or(&value).to_string();

            let mut last_value = card.get_value_clone();
            last_value = last_value.strip_suffix("&").unwrap_or(&last_value).to_string();

            card.set_value(format!("{}{}", last_value, value));
        }
    }

}