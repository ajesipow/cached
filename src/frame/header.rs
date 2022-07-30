use crate::domain::TTLSinceUnixEpochInMillis;
use crate::error::{Error, FrameError, Result};
use crate::primitives::{OpCode, Status};
use bytes::{Buf, Bytes};

pub(crate) static HEADER_SIZE_BYTES: u8 = 23;

pub(crate) trait Header {
    fn get_op_code(&self) -> OpCode;
    fn get_key_length(&self) -> u8;
    fn get_status_or_padding(&self) -> u8;
    fn get_ttl_since_unix_epoch_in_millis(&self) -> TTLSinceUnixEpochInMillis;
    fn get_total_frame_length(&self) -> u32;
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct RequestHeader(RequestHeaderInner);

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
struct RequestHeaderInner {
    pub op_code: OpCode,
    /// Can be ignored
    pub padding: u8,
    pub key_length: u8,
    pub ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
    pub total_frame_length: u32,
}

impl RequestHeader {
    pub fn parse(
        op_code: OpCode,
        key: Option<&str>,
        value: Option<&str>,
        ttl_since_unix_epoch_in_millis: Option<u128>,
    ) -> Result<Self> {
        let (key_length, total_frame_length) = get_key_and_total_frame_length(key, value)?;
        let ttl = TTLSinceUnixEpochInMillis::parse(ttl_since_unix_epoch_in_millis);
        Ok(Self(RequestHeaderInner {
            op_code,
            padding: 0,
            key_length,
            ttl_since_unix_epoch_in_millis: ttl,
            total_frame_length,
        }))
    }

    pub fn get_opcode(&self) -> &OpCode {
        &self.0.op_code
    }
}
impl Header for RequestHeader {
    fn get_op_code(&self) -> OpCode {
        self.0.op_code
    }

    fn get_key_length(&self) -> u8 {
        self.0.key_length
    }

    fn get_status_or_padding(&self) -> u8 {
        self.0.padding
    }

    fn get_ttl_since_unix_epoch_in_millis(&self) -> TTLSinceUnixEpochInMillis {
        self.0.ttl_since_unix_epoch_in_millis
    }

    fn get_total_frame_length(&self) -> u32 {
        self.0.total_frame_length
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ResponseHeader(ResponseHeaderInner);

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
struct ResponseHeaderInner {
    pub op_code: OpCode,
    pub status: Status,
    pub key_length: u8,
    pub ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
    pub total_frame_length: u32,
}

impl ResponseHeader {
    pub fn parse(
        op_code: OpCode,
        status: Status,
        key: Option<&str>,
        value: Option<&str>,
    ) -> Result<Self> {
        let (key_length, total_frame_length) = get_key_and_total_frame_length(key, value)?;
        Ok(Self(ResponseHeaderInner {
            op_code,
            status,
            key_length,
            // FIXME
            ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis::parse(Some(0)),
            total_frame_length,
        }))
    }

    pub fn get_opcode(&self) -> &OpCode {
        &self.0.op_code
    }

    pub fn get_status(&self) -> &Status {
        &self.0.status
    }
}

impl Header for ResponseHeader {
    fn get_op_code(&self) -> OpCode {
        self.0.op_code
    }

    fn get_key_length(&self) -> u8 {
        self.0.key_length
    }

    fn get_status_or_padding(&self) -> u8 {
        self.0.status as u8
    }

    fn get_ttl_since_unix_epoch_in_millis(&self) -> TTLSinceUnixEpochInMillis {
        self.0.ttl_since_unix_epoch_in_millis
    }

    fn get_total_frame_length(&self) -> u32 {
        self.0.total_frame_length
    }
}

impl TryFrom<Bytes> for RequestHeader {
    type Error = Error;

    fn try_from(mut value: Bytes) -> Result<Self> {
        if value.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(Error::Frame(FrameError::Incomplete));
        }
        let op_code = OpCode::try_from(value.get_u8())?;
        let padding = value.get_u8();
        let key_length = value.get_u8();
        let ttl_since_unix_epoch_in_millis =
            TTLSinceUnixEpochInMillis::parse(Some(value.get_u128()));
        let total_frame_length = value.get_u32();

        Ok(Self(RequestHeaderInner {
            op_code,
            key_length,
            padding,
            ttl_since_unix_epoch_in_millis,
            total_frame_length,
        }))
    }
}

impl TryFrom<Bytes> for ResponseHeader {
    type Error = Error;

    fn try_from(mut value: Bytes) -> Result<Self> {
        if value.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(Error::Frame(FrameError::Incomplete));
        }
        let op_code = OpCode::try_from(value.get_u8())?;
        let status = Status::try_from(value.get_u8())?;
        let key_length = value.get_u8();
        let ttl_since_unix_epoch_in_millis =
            TTLSinceUnixEpochInMillis::parse(Some(value.get_u128()));
        let total_frame_length = value.get_u32();

        Ok(Self(ResponseHeaderInner {
            op_code,
            status,
            key_length,
            ttl_since_unix_epoch_in_millis,
            total_frame_length,
        }))
    }
}

/// Key must not be longer than u8::MAX
fn validate_key_length(key: Option<&str>) -> Result<u8> {
    let key_length = key.map_or(0, |s| s.len());
    if key_length > u8::MAX as usize {
        return Err(Error::Frame(FrameError::KeyTooLong));
    }
    Ok(key_length as u8)
}

/// Value must not greater than 1MB
fn validate_value_length(value: Option<&str>) -> Result<u32> {
    let value_length = value.map_or(0, |s| s.len());
    let max_value_length = 1024 * 1024;
    if value_length > max_value_length as usize {
        return Err(Error::Frame(FrameError::ValueTooLong));
    }
    Ok(value_length as u32)
}

fn get_key_and_total_frame_length(key: Option<&str>, value: Option<&str>) -> Result<(u8, u32)> {
    let key_length = validate_key_length(key)?;
    let value_length = validate_value_length(value)?;
    let total_frame_length = HEADER_SIZE_BYTES as u32 + key_length as u32 + value_length;
    Ok((key_length, total_frame_length))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parsing_request_header_with_valid_long_key_works() {
        let key = "a".repeat(u8::MAX as usize);
        assert!(
            RequestHeader::parse(OpCode::Get, Some(key.as_str()), None, None).is_ok(),
            "Was not able to parse a valid long key!"
        );
    }

    #[test]
    fn test_parsing_request_header_with_too_long_key_fails() {
        let key = "a".repeat(u8::MAX as usize + 1);
        assert_eq!(
            RequestHeader::parse(OpCode::Get, Some(key.as_str()), None, None),
            Err(Error::Frame(FrameError::KeyTooLong))
        );
    }

    #[test]
    fn test_parsing_request_header_with_valid_long_value_works() {
        let value = "a".repeat((1024 * 1024) as usize);
        assert!(
            RequestHeader::parse(OpCode::Get, None, Some(value.as_str()), None).is_ok(),
            "Was not able to parse a valid long value!"
        );
    }

    #[test]
    fn test_parsing_request_header_with_too_long_value_fails() {
        let value = "a".repeat((1024 * 1024) as usize + 1);
        assert_eq!(
            RequestHeader::parse(OpCode::Get, None, Some(value.as_str()), None),
            Err(Error::Frame(FrameError::ValueTooLong))
        );
    }

    #[test]
    fn test_parsing_request_header_with_valid_long_key_and_value_works() {
        let key = "a".repeat(u8::MAX as usize);
        let value = "a".repeat((1024 * 1024) as usize);
        assert!(
            RequestHeader::parse(OpCode::Get, Some(key.as_str()), Some(value.as_str()), None)
                .is_ok(),
            "Was not able to parse a valid long value!"
        );
    }

    #[test]
    fn test_parsing_response_header_with_valid_long_key_works() {
        let key = "a".repeat(u8::MAX as usize);
        assert!(
            ResponseHeader::parse(OpCode::Get, Status::Ok, Some(key.as_str()), None).is_ok(),
            "Was not able to parse a valid long key!"
        );
    }

    #[test]
    fn test_parsing_response_header_with_too_long_key_fails() {
        let key = "a".repeat(u8::MAX as usize + 1);
        assert_eq!(
            ResponseHeader::parse(OpCode::Get, Status::Ok, Some(key.as_str()), None),
            Err(Error::Frame(FrameError::KeyTooLong))
        );
    }

    #[test]
    fn test_parsing_response_header_with_valid_long_value_works() {
        let value = "a".repeat((1024 * 1024) as usize);
        assert!(
            ResponseHeader::parse(OpCode::Get, Status::Ok, None, Some(value.as_str())).is_ok(),
            "Was not able to parse a valid long value!"
        );
    }

    #[test]
    fn test_parsing_response_header_with_too_long_value_fails() {
        let value = "a".repeat((1024 * 1024) as usize + 1);
        assert_eq!(
            ResponseHeader::parse(OpCode::Get, Status::Ok, None, Some(value.as_str())),
            Err(Error::Frame(FrameError::ValueTooLong))
        );
    }

    #[test]
    fn test_parsing_response_header_with_valid_long_key_and_value_works() {
        let key = "a".repeat(u8::MAX as usize);
        let value = "a".repeat((1024 * 1024) as usize);
        assert!(
            ResponseHeader::parse(
                OpCode::Get,
                Status::Ok,
                Some(key.as_str()),
                Some(value.as_str())
            )
            .is_ok(),
            "Was not able to parse a valid long value!"
        );
    }
}
