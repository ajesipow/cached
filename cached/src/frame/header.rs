use crate::domain::TTLSinceUnixEpochInMillis;
use crate::error::{Error, FrameError, Result};
use crate::primitives::{OpCode, Status};
use bytes::{Buf, Bytes};

static HEADER_SIZE_BYTES: u8 = 23;

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct RequestHeader {
    pub op_code: OpCode,
    pub key_length: u8,
    pub ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
    pub total_frame_length: u32,
}

impl RequestHeader {
    pub fn new(
        op_code: OpCode,
        key_length: u8,
        total_frame_length: u32,
        ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
    ) -> Self {
        Self {
            op_code,
            key_length,
            ttl_since_unix_epoch_in_millis,
            total_frame_length,
        }
    }

    pub fn size() -> u8 {
        HEADER_SIZE_BYTES
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ResponseHeader {
    pub op_code: OpCode,
    pub status: Status,
    pub key_length: u8,
    pub ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
    pub total_frame_length: u32,
}

impl ResponseHeader {
    pub fn new(
        op_code: OpCode,
        status: Status,
        key_length: u8,
        total_frame_length: u32,
        ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
    ) -> Self {
        Self {
            op_code,
            status,
            key_length,
            ttl_since_unix_epoch_in_millis,
            total_frame_length,
        }
    }

    pub fn size() -> u8 {
        HEADER_SIZE_BYTES
    }
}

impl TryFrom<Bytes> for RequestHeader {
    type Error = Error;

    fn try_from(mut value: Bytes) -> Result<Self> {
        if value.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(Error::Frame(FrameError::Incomplete));
        }
        let op_code = OpCode::try_from(value.get_u8())?;
        let _ = value.get_u8();
        let key_length = value.get_u8();
        let ttl_since_unix_epoch_in_millis =
            TTLSinceUnixEpochInMillis::parse(Some(value.get_u128()));
        let total_frame_length = value.get_u32();

        Ok(Self {
            op_code,
            key_length,
            ttl_since_unix_epoch_in_millis,
            total_frame_length,
        })
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

        Ok(Self {
            op_code,
            status,
            key_length,
            ttl_since_unix_epoch_in_millis,
            total_frame_length,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::domain::{Key, Value};

    #[test]
    fn test_parsing_request_with_valid_long_key_works() {
        let key = "a".repeat(u8::MAX as usize);
        assert!(
            Key::parse(key).is_ok(),
            "Was not able to parse a valid long key!"
        );
    }

    #[test]
    fn test_parsing_request_with_too_long_key_fails() {
        let key = "a".repeat(u8::MAX as usize + 1);
        assert_eq!(Key::parse(key), Err(Error::Frame(FrameError::KeyTooLong)));
    }

    #[test]
    fn test_parsing_request_header_with_valid_long_value_works() {
        let value = "a".repeat((1024 * 1024) as usize);
        assert!(
            Value::parse(value).is_ok(),
            "Was not able to parse a valid long value!"
        );
    }

    #[test]
    fn test_parsing_request_header_with_too_long_value_fails() {
        let value = "a".repeat((1024 * 1024) as usize + 1);
        assert_eq!(
            Value::parse(value),
            Err(Error::Frame(FrameError::ValueTooLong))
        );
    }
}
