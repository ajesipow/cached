use crate::error::Error;
use crate::error::FrameError;
use crate::error::Result;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum StatusCode {
    Ok = 0,
    KeyNotFound = 1,
    KeyExists = 2,
    InternalError = 3,
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => write!(f, "OK"),
            Self::KeyNotFound => write!(f, "Key not found"),
            Self::KeyExists => write!(f, "Key exists"),
            Self::InternalError => write!(f, "INTERNAL ERROR"),
        }
    }
}

impl TryFrom<u8> for StatusCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(StatusCode::Ok),
            1 => Ok(StatusCode::KeyNotFound),
            2 => Ok(StatusCode::KeyExists),
            3 => Ok(StatusCode::InternalError),
            _ => Err(Error::Frame(FrameError::InvalidStatusCode)),
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[repr(u8)]
pub enum OpCode {
    Set = 1,
    Get = 2,
    Delete = 3,
    Flush = 4,
}

impl TryFrom<u8> for OpCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(OpCode::Set),
            2 => Ok(OpCode::Get),
            3 => Ok(OpCode::Delete),
            4 => Ok(OpCode::Flush),
            _ => Err(Error::Frame(FrameError::InvalidOpCode)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::rstest;

    #[test]
    fn test_op_code_serialisation() {
        assert_eq!(OpCode::Set as u8, 1);
        assert_eq!(OpCode::Get as u8, 2);
        assert_eq!(OpCode::Delete as u8, 3);
        assert_eq!(OpCode::Flush as u8, 4);
    }

    #[test]
    fn test_op_code_deserialisation_works() {
        assert_eq!(OpCode::try_from(1), Ok(OpCode::Set));
        assert_eq!(OpCode::try_from(2), Ok(OpCode::Get));
        assert_eq!(OpCode::try_from(3), Ok(OpCode::Delete));
        assert_eq!(OpCode::try_from(4), Ok(OpCode::Flush));
    }

    #[rstest]
    #[case(0)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    fn test_op_code_deserialization_fails_for_wrong_codes(#[case] input: u8) {
        assert!(OpCode::try_from(input).is_err());
    }

    #[test]
    fn test_status_code_serialization() {
        assert_eq!(StatusCode::Ok as u8, 0);
        assert_eq!(StatusCode::KeyNotFound as u8, 1);
        assert_eq!(StatusCode::KeyExists as u8, 2);
        assert_eq!(StatusCode::InternalError as u8, 3);
    }

    #[test]
    fn test_status_code_deserialization_works() {
        assert_eq!(StatusCode::try_from(0), Ok(StatusCode::Ok));
        assert_eq!(StatusCode::try_from(1), Ok(StatusCode::KeyNotFound));
        assert_eq!(StatusCode::try_from(2), Ok(StatusCode::KeyExists));
        assert_eq!(StatusCode::try_from(3), Ok(StatusCode::InternalError));
    }

    #[rstest]
    #[case(4)]
    #[case(5)]
    #[case(6)]
    #[case(7)]
    #[case(8)]
    #[case(9)]
    #[case(10)]
    fn test_status_code_deserialization_fails_for_wrong_codes(#[case] input: u8) {
        assert!(StatusCode::try_from(input).is_err());
    }
}
