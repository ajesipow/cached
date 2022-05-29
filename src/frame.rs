#![allow(dead_code)]

use bytes::{Buf, Bytes};
use std::fmt::Debug;
use std::io::Cursor;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum FrameError {
    Incomplete,
    InvalidOpCode,
    InvalidStatusCode,
}

static HEADER_SIZE_BYTES: u8 = 7;

pub trait Header {
    fn get_op_code(&self) -> OpCode;
    fn get_key_length(&self) -> u8;
    fn get_status_or_blank(&self) -> u8;
    fn get_total_frame_length(&self) -> u32;
}

pub trait Frame {
    type Header: Header + TryFrom<Bytes, Error = FrameError> + Debug;

    fn new(header: Self::Header, key: Option<String>, value: Option<String>) -> Self;
    fn get_header(&self) -> &Self::Header;
    fn get_key(&self) -> Option<&str>;
    fn get_value(&self) -> Option<&str>;

    fn check(src: &mut Cursor<&[u8]>) -> Result<usize, FrameError> {
        if src.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(FrameError::Incomplete);
        }
        let header = Self::Header::try_from(src.copy_to_bytes(HEADER_SIZE_BYTES as usize))?;

        if src.remaining() < header.get_total_frame_length() as usize - HEADER_SIZE_BYTES as usize {
            return Err(FrameError::Incomplete);
        }
        Ok(header.get_total_frame_length() as usize)
    }

    fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, FrameError>
    where
        Self: Sized,
    {
        let header = Self::Header::try_from(src.copy_to_bytes(HEADER_SIZE_BYTES as usize))?;
        let key_length = header.get_key_length();
        let value_length =
            header.get_total_frame_length() - HEADER_SIZE_BYTES as u32 - key_length as u32;
        let key = match key_length {
            0 => None,
            key_length => Some(get_string(src, key_length as u32)?),
        };
        let value = match value_length {
            0 => None,
            value_length => Some(get_string(src, value_length)?),
        };
        Ok(Self::new(header, key, value))
    }
}

#[derive(Debug)]
pub struct ResponseFrame {
    pub header: ResponseHeader,
    pub key: Option<String>,
    pub value: Option<String>,
}

impl Frame for ResponseFrame {
    type Header = ResponseHeader;

    fn new(header: Self::Header, key: Option<String>, value: Option<String>) -> Self {
        Self { header, key, value }
    }

    fn get_header(&self) -> &Self::Header {
        &self.header
    }

    fn get_key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn get_value(&self) -> Option<&str> {
        self.value.as_deref()
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct RequestFrame {
    pub header: RequestHeader,
    pub key: Option<String>,
    pub value: Option<String>,
}

impl Frame for RequestFrame {
    type Header = RequestHeader;

    fn new(header: Self::Header, key: Option<String>, value: Option<String>) -> Self {
        Self { header, key, value }
    }
    fn get_header(&self) -> &Self::Header {
        &self.header
    }

    fn get_key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn get_value(&self) -> Option<&str> {
        self.value.as_deref()
    }
}

fn get_string(src: &mut Cursor<&[u8]>, len: u32) -> Result<String, FrameError> {
    if src.remaining() < len as usize {
        return Err(FrameError::Incomplete);
    }
    let value = String::from_utf8_lossy(src.take(len as usize).chunk()).to_string();
    let new_position = src.position() + len as u64;
    src.set_position(new_position);
    Ok(value)
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
// TODO: newtype idiom?
pub struct RequestHeader {
    pub op_code: OpCode,
    /// Can be ignored
    pub blank: u8,
    pub key_length: u8,
    pub total_frame_length: u32,
}

impl RequestHeader {
    pub fn new(op_code: OpCode, key: Option<&str>, value: Option<&str>) -> Self {
        let value_length = value.map_or(0, |s| s.len() as u32);
        let key_length = key.map_or(0, |s| s.len() as u8);
        Self {
            op_code,
            blank: 0,
            // TODO key must not be longer than u8
            key_length,
            // TODO value must not be longer than u32
            total_frame_length: HEADER_SIZE_BYTES as u32 + key_length as u32 + value_length,
        }
    }
}

impl Header for RequestHeader {
    fn get_op_code(&self) -> OpCode {
        self.op_code
    }

    fn get_key_length(&self) -> u8 {
        self.key_length
    }

    // TODO rename
    fn get_status_or_blank(&self) -> u8 {
        self.blank
    }

    fn get_total_frame_length(&self) -> u32 {
        self.total_frame_length
    }
}

#[derive(Debug)]
pub struct ResponseHeader {
    pub op_code: OpCode,
    pub status: Status,
    pub key_length: u8,
    pub total_frame_length: u32,
}

impl ResponseHeader {
    pub fn new(op_code: OpCode, status: Status, key: Option<&str>, value: Option<&str>) -> Self {
        let value_length = value.map_or(0, |s| s.len() as u32);
        let key_length = key.map_or(0, |s| s.len() as u8);
        Self {
            op_code,
            status,
            // TODO key must not be longer than u8
            key_length,
            // TODO value must not be longer than u32
            total_frame_length: HEADER_SIZE_BYTES as u32 + key_length as u32 + value_length,
        }
    }
}

impl Header for ResponseHeader {
    fn get_op_code(&self) -> OpCode {
        self.op_code
    }
    fn get_key_length(&self) -> u8 {
        self.key_length
    }

    fn get_status_or_blank(&self) -> u8 {
        self.status as u8
    }

    fn get_total_frame_length(&self) -> u32 {
        self.total_frame_length
    }
}

impl TryFrom<Bytes> for RequestHeader {
    type Error = FrameError;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        if value.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(FrameError::Incomplete);
        }
        let op_code = OpCode::try_from(value.get_u8())?;
        let blank = value.get_u8();
        let key_length = value.get_u8();
        let total_frame_length = value.get_u32();

        Ok(Self {
            op_code,
            key_length,
            blank,
            total_frame_length,
        })
    }
}

impl TryFrom<Bytes> for ResponseHeader {
    type Error = FrameError;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        if value.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(FrameError::Incomplete);
        }
        let op_code = OpCode::try_from(value.get_u8())?;
        let status = Status::try_from(value.get_u8())?;
        let key_length = value.get_u8();
        let total_frame_length = value.get_u32();

        Ok(Self {
            op_code,
            status,
            key_length,
            total_frame_length,
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    KeyNotFound = 1,
    KeyExists = 2,
    InternalError = 3,
}

impl TryFrom<u8> for Status {
    type Error = FrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Status::Ok),
            1 => Ok(Status::KeyNotFound),
            2 => Ok(Status::KeyExists),
            3 => Ok(Status::InternalError),
            _ => Err(FrameError::InvalidStatusCode),
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
    type Error = FrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(OpCode::Set),
            2 => Ok(OpCode::Get),
            3 => Ok(OpCode::Delete),
            4 => Ok(OpCode::Flush),
            _ => Err(FrameError::InvalidOpCode),
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
