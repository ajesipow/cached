use crate::error::{Error, FrameError, Result};
use bytes::{Buf, Bytes};
use std::fmt::Debug;

use crate::domain::{Key, TTLSinceUnixEpochInMillis, Value};
use crate::primitives::OpCode;
use crate::StatusCode;

static HEADER_SIZE_BYTES: u8 = 23;

#[derive(Debug)]
pub(crate) struct ResponseFrame {
    pub header: ResponseHeader,
    pub key: Option<Key>,
    pub value: Option<Value>,
}

impl ResponseFrame {
    pub(crate) fn new(
        op_code: OpCode,
        status: StatusCode,
        ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
        key: Option<Key>,
        value: Option<Value>,
    ) -> Result<Self> {
        let key_length = key.as_ref().map_or(0, |k| k.len());
        let value_length = value.as_ref().map_or(0, |v| v.len());
        // TODO?
        // We're assuming no overflow here as value should be sufficiently smaller than u32:MAX - 2*u8::MAX
        let total_frame_length = ResponseHeader::size() as u32 + key_length as u32 + value_length;
        let header = ResponseHeader::new(
            op_code,
            status,
            key_length,
            total_frame_length,
            ttl_since_unix_epoch_in_millis,
        );
        Ok(Self { header, key, value })
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct RequestFrame {
    pub header: RequestHeader,
    pub key: Option<Key>,
    pub value: Option<Value>,
}

impl RequestFrame {
    pub(crate) fn new(
        op_code: OpCode,
        ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
        key: Option<Key>,
        value: Option<Value>,
    ) -> Result<Self> {
        let key_length = key.as_ref().map_or(0, |k| k.len());
        let value_length = value.as_ref().map_or(0, |v| v.len());
        // TODO?
        // We're assuming no overflow here as value should be sufficiently smaller than u32:MAX - 2*u8::MAX
        let total_frame_length = ResponseHeader::size() as u32 + key_length as u32 + value_length;
        let header = RequestHeader::new(
            op_code,
            key_length,
            total_frame_length,
            ttl_since_unix_epoch_in_millis,
        );
        Ok(Self { header, key, value })
    }
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct RequestHeader {
    pub op_code: OpCode,
    pub key_length: u8,
    pub ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
    pub total_frame_length: u32,
}

impl RequestHeader {
    fn new(
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

    pub(crate) fn size() -> u8 {
        HEADER_SIZE_BYTES
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct ResponseHeader {
    pub op_code: OpCode,
    pub status: StatusCode,
    pub key_length: u8,
    pub ttl_since_unix_epoch_in_millis: TTLSinceUnixEpochInMillis,
    pub total_frame_length: u32,
}

impl ResponseHeader {
    fn new(
        op_code: OpCode,
        status: StatusCode,
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

    pub(crate) fn size() -> u8 {
        HEADER_SIZE_BYTES
    }
}

impl TryFrom<Bytes> for RequestHeader {
    type Error = Error;

    fn try_from(mut value: Bytes) -> Result<Self> {
        if value.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(Error::new_frame(FrameError::Incomplete));
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
            return Err(Error::new_frame(FrameError::Incomplete));
        }
        let op_code = OpCode::try_from(value.get_u8())?;
        let status = StatusCode::try_from(value.get_u8())?;
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
    use crate::error::ErrorInner;

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
        assert!(matches!(
            Key::parse(key),
            Err(Error(ErrorInner::Frame(FrameError::KeyTooLong)))
        ));
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
        assert!(matches!(
            Value::parse(value),
            Err(Error(ErrorInner::Frame(FrameError::ValueTooLong)))
        ));
    }
}
