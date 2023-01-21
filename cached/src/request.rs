use crate::domain::{Key, TTLSinceUnixEpochInMillis, Value};
use crate::error::{Error, ParseError};
use crate::frame::RequestFrame;
use crate::primitives::OpCode;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum Request {
    Get(Key),
    Set {
        key: Key,
        value: Value,
        ttl_since_unix_epoch_in_millis: Option<u128>,
    },
    Delete(Key),
    Flush,
}

impl TryFrom<Request> for RequestFrame {
    type Error = Error;

    fn try_from(req: Request) -> Result<Self, Self::Error> {
        let (op_code, ttl, key, value) = match req {
            Request::Get(key) => (OpCode::Get, None, Some(key), None),
            Request::Set {
                key,
                value,
                ttl_since_unix_epoch_in_millis,
            } => (
                OpCode::Set,
                ttl_since_unix_epoch_in_millis,
                Some(key),
                Some(value),
            ),
            Request::Delete(key) => (OpCode::Delete, None, Some(key), None),
            Request::Flush => (OpCode::Flush, None, None, None),
        };

        let ttl = TTLSinceUnixEpochInMillis::parse(ttl);
        RequestFrame::new(op_code, ttl, key, value)
    }
}

impl TryFrom<RequestFrame> for Request {
    type Error = Error;

    fn try_from(frame: RequestFrame) -> Result<Self, Self::Error> {
        match frame.header.op_code {
            OpCode::Set => Ok(Request::Set {
                key: frame.key.ok_or(Error::Parse(ParseError::KeyMissing))?,
                value: frame.value.ok_or(Error::Parse(ParseError::ValueMissing))?,
                ttl_since_unix_epoch_in_millis: frame
                    .header
                    .ttl_since_unix_epoch_in_millis
                    .into_ttl(),
            }),
            OpCode::Get => {
                if frame.value.is_some() {
                    return Err(Error::Parse(ParseError::UnexpectedValue));
                }
                Ok(Request::Get(
                    frame.key.ok_or(Error::Parse(ParseError::KeyMissing))?,
                ))
            }
            OpCode::Delete => {
                if frame.value.is_some() {
                    return Err(Error::Parse(ParseError::UnexpectedValue));
                }
                Ok(Request::Delete(
                    frame.key.ok_or(Error::Parse(ParseError::KeyMissing))?,
                ))
            }
            OpCode::Flush => {
                if frame.key.is_some() {
                    return Err(Error::Parse(ParseError::UnexpectedKey));
                }
                if frame.value.is_some() {
                    return Err(Error::Parse(ParseError::UnexpectedValue));
                }
                Ok(Request::Flush)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::domain::TTLSinceUnixEpochInMillis;
    use crate::Request;
    use rstest::rstest;

    #[rstest]
    #[case(
        OpCode::Get,
        Some("ABC".to_string()),
        None,
        Request::Get(Key::parse("ABC".to_string()).unwrap())
    )]
    #[case(
        OpCode::Set,
        Some("ABC".to_string()),
        Some("Some value".to_string()),
        Request::Set {key: Key::parse("ABC".to_string()).unwrap(), value: Value::parse("Some value".to_string()).unwrap(), ttl_since_unix_epoch_in_millis: None }
    )]
    #[case(
        OpCode::Delete,
        Some("ABC".to_string()),
        None,
        Request::Delete(Key::parse("ABC".to_string()).unwrap())
    )]
    #[case(OpCode::Flush, None, None, Request::Flush)]
    fn test_conversion_from_valid_request_frame_to_request_works(
        #[case] op_code: OpCode,
        #[case] key: Option<String>,
        #[case] value: Option<String>,
        #[case] expected_request: Request,
    ) {
        let key = key.map(Key::parse).transpose().unwrap();
        let value = value.map(Value::parse).transpose().unwrap();
        let ttl = TTLSinceUnixEpochInMillis::parse(None);
        let req_frame = RequestFrame::new(op_code, ttl, key, value).unwrap();
        assert_eq!(Request::try_from(req_frame), Ok(expected_request))
    }

    #[rstest]
    #[case(OpCode::Get, None, None)]
    #[case(OpCode::Get, None, Some("ABC".to_string()))]
    #[case(OpCode::Get,
        Some("ABC".to_string()),
        Some("Some value".to_string()))]
    #[case(
        OpCode::Set,
        Some("ABC".to_string()),
        None,
    )]
    #[case(
        OpCode::Set,
        None,
        Some("Some value".to_string()),
    )]
    #[case(OpCode::Set, None, None)]
    #[case(OpCode::Delete, None, None)]
    #[case(OpCode::Delete, None, Some("Some value".to_string()))]
    #[case(OpCode::Flush,
        Some("ABC".to_string()),
        Some("Some value".to_string()))]
    #[case(
        OpCode::Flush,
        Some("ABC".to_string()),
        None,
    )]
    #[case(
        OpCode::Flush,
        None,
        Some("Some value".to_string()),
    )]
    fn test_conversion_from_invalid_request_frame_to_request_fails(
        #[case] op_code: OpCode,
        #[case] key: Option<String>,
        #[case] value: Option<String>,
    ) {
        let key = key.map(Key::parse).transpose().unwrap();
        let value = value.map(Value::parse).transpose().unwrap();
        let ttl = TTLSinceUnixEpochInMillis::parse(None);
        let req_frame = RequestFrame::new(op_code, ttl, key, value).unwrap();
        assert!(Request::try_from(req_frame).is_err())
    }
}
