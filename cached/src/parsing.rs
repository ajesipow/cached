use crate::domain::{Key, TTLSinceUnixEpochInMillis, Value};
use crate::error::{FrameError, Parse, Result};
use crate::frame::header::{RequestHeader, ResponseHeader};
use crate::frame::{RequestFrame, ResponseFrame};
use crate::primitives::OpCode;
use crate::{Error, StatusCode};
use nom::bytes::streaming::take;
use nom::combinator::map_res;
use nom::number::streaming::{be_u128, be_u32, u8};
use nom::IResult;

pub(crate) fn parse_request_frame(input: &[u8]) -> Result<RequestFrame> {
    let (
        _,
        RequestPrimitive {
            op_code,
            ttl_since_unix_epoch_in_millis,
            key_bytes,
            value_bytes,
        },
    ) = parse_request_primitives(input).map_err(|e| {
        if e.is_incomplete() {
            Error::Frame(FrameError::Incomplete)
        } else {
            Error::Parse(Parse::String)
        }
    })?;
    let key = match key_bytes.len() {
        0 => None,
        // TODO use Cow instead?
        _ => {
            let key =
                String::from_utf8(key_bytes.to_vec()).map_err(|_| Error::Parse(Parse::String))?;
            let key = Key::parse(key)?;
            Some(key)
        }
    };
    let value = match value_bytes.len() {
        0 => None,
        // TODO use Cow instead?
        _ => {
            let value =
                String::from_utf8(value_bytes.to_vec()).map_err(|_| Error::Parse(Parse::String))?;
            let value = Value::parse(value)?;
            Some(value)
        }
    };
    let ttl_since_unix_epoch_in_millis =
        TTLSinceUnixEpochInMillis::parse(Some(ttl_since_unix_epoch_in_millis));
    RequestFrame::new(op_code, ttl_since_unix_epoch_in_millis, key, value)
}

struct RequestPrimitive<'a> {
    op_code: OpCode,
    ttl_since_unix_epoch_in_millis: u128,
    key_bytes: &'a [u8],
    value_bytes: &'a [u8],
}

fn parse_request_primitives(input: &[u8]) -> IResult<&[u8], RequestPrimitive<'_>> {
    let (remainder, op_code) = map_res(u8, OpCode::try_from)(input)?;
    let (remainder, _) = u8(remainder)?;
    let (remainder, key_length) = u8(remainder)?;
    let (remainder, ttl_since_unix_epoch_in_millis) = be_u128(remainder)?;
    let (remainder, total_frame_length) = be_u32(remainder)?;
    let key_length = key_length as usize;
    let (remainder, key_bytes) = take(key_length)(remainder)?;
    let value_length = total_frame_length as usize - RequestHeader::size() as usize - key_length;
    let (remainder, value_bytes) = take(value_length)(remainder)?;
    Ok((
        remainder,
        RequestPrimitive {
            op_code,
            ttl_since_unix_epoch_in_millis,
            key_bytes,
            value_bytes,
        },
    ))
}

pub(crate) fn parse_response_frame(input: &[u8]) -> Result<ResponseFrame> {
    let (
        _,
        ResponsePrimitive {
            op_code,
            status,
            ttl_since_unix_epoch_in_millis,
            key_bytes,
            value_bytes,
        },
    ) = parse_response_primitives(input).map_err(|e| {
        if e.is_incomplete() {
            Error::Frame(FrameError::Incomplete)
        } else {
            Error::Parse(Parse::String)
        }
    })?;
    let key = match key_bytes.len() {
        0 => None,
        // TODO use Cow instead?
        _ => {
            let key =
                String::from_utf8(key_bytes.to_vec()).map_err(|_| Error::Parse(Parse::String))?;
            let key = Key::parse(key)?;
            Some(key)
        }
    };
    let value = match value_bytes.len() {
        0 => None,
        // TODO use Cow instead?
        _ => {
            let value =
                String::from_utf8(value_bytes.to_vec()).map_err(|_| Error::Parse(Parse::String))?;
            let value = Value::parse(value)?;
            Some(value)
        }
    };
    let ttl_since_unix_epoch_in_millis =
        TTLSinceUnixEpochInMillis::parse(Some(ttl_since_unix_epoch_in_millis));
    ResponseFrame::new(op_code, status, ttl_since_unix_epoch_in_millis, key, value)
}

struct ResponsePrimitive<'a> {
    op_code: OpCode,
    status: StatusCode,
    ttl_since_unix_epoch_in_millis: u128,
    key_bytes: &'a [u8],
    value_bytes: &'a [u8],
}

fn parse_response_primitives(input: &[u8]) -> IResult<&[u8], ResponsePrimitive<'_>> {
    let (remainder, op_code) = map_res(u8, OpCode::try_from)(input)?;
    let (remainder, status) = map_res(u8, StatusCode::try_from)(remainder)?;
    let (remainder, key_length) = u8(remainder)?;
    let (remainder, ttl_since_unix_epoch_in_millis) = be_u128(remainder)?;
    let (remainder, total_frame_length) = be_u32(remainder)?;
    let (remainder, key_bytes) = take(key_length)(remainder)?;
    let value_length =
        total_frame_length as usize - ResponseHeader::size() as usize - key_length as usize;
    let (_, value_bytes) = take(value_length)(remainder)?;
    Ok((
        remainder,
        ResponsePrimitive {
            op_code,
            status,
            ttl_since_unix_epoch_in_millis,
            key_bytes,
            value_bytes,
        },
    ))
}
