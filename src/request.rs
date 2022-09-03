use crate::error::{Error, Parse};
use crate::frame::{Frame, RequestFrame};
use crate::primitives::OpCode;

use crate::frame::header::Header;
use crate::frame::header::RequestHeader;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum Request {
    Get(String),
    Set {
        key: String,
        value: String,
        ttl_since_unix_epoch_in_millis: Option<u128>,
    },
    Delete(String),
    Flush,
}

impl TryFrom<Request> for RequestFrame {
    type Error = Error;

    fn try_from(req: Request) -> Result<Self, Self::Error> {
        let (header, key, value) = match req {
            Request::Get(key) => {
                let header = RequestHeader::parse(OpCode::Get, Some(&key), None, None)?;
                (header, Some(key), None)
            }
            Request::Set {
                key,
                value,
                ttl_since_unix_epoch_in_millis,
            } => {
                let header = RequestHeader::parse(
                    OpCode::Set,
                    Some(&key),
                    Some(&value),
                    ttl_since_unix_epoch_in_millis,
                )?;
                (header, Some(key), Some(value))
            }
            Request::Delete(key) => {
                let header = RequestHeader::parse(OpCode::Delete, Some(&key), None, None)?;
                (header, Some(key), None)
            }
            Request::Flush => {
                let header = RequestHeader::parse(OpCode::Flush, None, None, None)?;
                (header, None, None)
            }
        };
        Ok(RequestFrame::new(header, key, value))
    }
}

impl TryFrom<RequestFrame> for Request {
    type Error = Error;

    fn try_from(frame: RequestFrame) -> Result<Self, Self::Error> {
        match frame.header.get_opcode() {
            OpCode::Set => Ok(Request::Set {
                key: frame.key.ok_or(Error::Parse(Parse::KeyMissing))?,
                value: frame.value.ok_or(Error::Parse(Parse::ValueMissing))?,
                ttl_since_unix_epoch_in_millis: frame
                    .header
                    .get_ttl_since_unix_epoch_in_millis()
                    .into_ttl(),
            }),
            OpCode::Get => {
                if frame.value.is_some() {
                    return Err(Error::Parse(Parse::UnexpectedValue));
                }
                Ok(Request::Get(
                    frame.key.ok_or(Error::Parse(Parse::KeyMissing))?,
                ))
            }
            OpCode::Delete => {
                if frame.value.is_some() {
                    return Err(Error::Parse(Parse::UnexpectedValue));
                }
                Ok(Request::Delete(
                    frame.key.ok_or(Error::Parse(Parse::KeyMissing))?,
                ))
            }
            OpCode::Flush => {
                if frame.key.is_some() {
                    return Err(Error::Parse(Parse::UnexpectedKey));
                }
                if frame.value.is_some() {
                    return Err(Error::Parse(Parse::UnexpectedValue));
                }
                Ok(Request::Flush)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Request;
    use rstest::rstest;

    #[rstest]
    #[case(
        OpCode::Get,
        Some("ABC".to_string()),
        None,
        Request::Get("ABC".to_string())
    )]
    #[case(
        OpCode::Set,
        Some("ABC".to_string()),
        Some("Some value".to_string()),
        Request::Set {key: "ABC".to_string(), value: "Some value".to_string(), ttl_since_unix_epoch_in_millis: None }
    )]
    #[case(
        OpCode::Delete,
        Some("ABC".to_string()),
        None,
        Request::Delete("ABC".to_string())
    )]
    #[case(OpCode::Flush, None, None, Request::Flush)]
    fn test_conversion_from_valid_request_frame_to_request_works(
        #[case] op_code: OpCode,
        #[case] key: Option<String>,
        #[case] value: Option<String>,
        #[case] expected_request: Request,
    ) {
        let req_frame = RequestFrame {
            header: RequestHeader::parse(op_code, key.as_deref(), value.as_deref(), None).unwrap(),
            key,
            value,
        };
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
        let req_frame = RequestFrame {
            header: RequestHeader::parse(op_code, key.as_deref(), value.as_deref(), None).unwrap(),
            key,
            value,
        };
        assert!(Request::try_from(req_frame).is_err())
    }
}
