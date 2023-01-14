use std::fmt::Debug;

use crate::error::Result;

pub(crate) mod header;
use crate::domain::{Key, TTLSinceUnixEpochInMillis, Value};
use crate::primitives::OpCode;
use crate::Status;
use header::*;

#[derive(Debug)]
pub struct ResponseFrame {
    pub header: ResponseHeader,
    pub key: Option<Key>,
    pub value: Option<Value>,
}

impl ResponseFrame {
    pub fn new(
        op_code: OpCode,
        status: Status,
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
pub struct RequestFrame {
    pub header: RequestHeader,
    pub key: Option<Key>,
    pub value: Option<Value>,
}

impl RequestFrame {
    pub fn new(
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
