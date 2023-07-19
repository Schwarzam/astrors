
struct Header {
    cards: Vec<Card>,
}

struct Card {
    keyword: String,
    value: String,
    comment: Option<String>,
}

impl Header {
    fn new() -> Self {
        Header {
            cards: Vec::new(),
        }
    }

    fn add_card(&mut self, card: Card) {
        self.cards.push(card);
    }

    fn get_card(&self, keyword: &str) -> Option<&Card> {
        for card in &self.cards {
            if card.keyword == keyword {
                return Some(card);
            }
        }

        None
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

    fn read_from_buffer(&mut self, &mut buf : [u8]){
        
    }


}