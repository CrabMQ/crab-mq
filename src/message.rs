#[derive(Debug, PartialEq)]
pub enum MessageType {
    Publish,
    Subscribe,
}

pub const MESSAGE_BUFFER_SIZE: usize = 1024;

pub const PUBLISH_BYTE: u8 = 0;
pub const SUBSCRIBE_BYTE: u8 = 1;

impl MessageType {
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            PUBLISH_BYTE => Some(Self::Publish),
            SUBSCRIBE_BYTE => Some(Self::Subscribe),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_type_from_byte() {
        assert_eq!(
            MessageType::from_byte(PUBLISH_BYTE),
            Some(MessageType::Publish)
        );
        assert_eq!(
            MessageType::from_byte(SUBSCRIBE_BYTE),
            Some(MessageType::Subscribe)
        );
        assert_eq!(MessageType::from_byte(69), None);
    }
}
