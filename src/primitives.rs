use crate::error::Error;
use crate::error::FrameError;
use crate::error::Result;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    KeyNotFound = 1,
    KeyExists = 2,
    InternalError = 3,
}

impl TryFrom<u8> for Status {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Status::Ok),
            1 => Ok(Status::KeyNotFound),
            2 => Ok(Status::KeyExists),
            3 => Ok(Status::InternalError),
            _ => Err(Error::Frame(FrameError::InvalidStatusCode)),
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
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
        assert_eq!(Status::Ok as u8, 0);
        assert_eq!(Status::KeyNotFound as u8, 1);
        assert_eq!(Status::KeyExists as u8, 2);
        assert_eq!(Status::InternalError as u8, 3);
    }

    #[test]
    fn test_status_code_deserialization_works() {
        assert_eq!(Status::try_from(0), Ok(Status::Ok));
        assert_eq!(Status::try_from(1), Ok(Status::KeyNotFound));
        assert_eq!(Status::try_from(2), Ok(Status::KeyExists));
        assert_eq!(Status::try_from(3), Ok(Status::InternalError));
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
        assert!(Status::try_from(input).is_err());
    }
}
