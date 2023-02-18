use crate::domain::{Key, TTLSinceUnixEpochInMillis, Value};
use crate::error::{Error, ParseError, Result};
use crate::frame::ResponseFrame;
use crate::primitives::{OpCode, StatusCode};
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Response {
    pub status: StatusCode,
    pub body: ResponseBody,
}

/// The response struct for a GET request.
///
/// The `value` is `None` if the key does not exist in the cache.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct ResponseGet {
    status: StatusCode,
    value: Option<String>,
    ttl_since_unix_epoch_in_millis: Option<u128>,
}

impl ResponseGet {
    pub(crate) fn new(
        status: StatusCode,
        value: Option<String>,
        ttl_since_unix_epoch_in_millis: Option<u128>,
    ) -> Self {
        Self {
            status,
            value,
            ttl_since_unix_epoch_in_millis,
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn ttl_since_unix_epoch_in_millis(&self) -> Option<u128> {
        self.ttl_since_unix_epoch_in_millis
    }

    pub fn value(&self) -> Option<&String> {
        self.value.as_ref()
    }

    pub fn into_value(self) -> Option<String> {
        self.value
    }
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.status {
            StatusCode::Ok => write!(f, "{}", self.body),
            _ => write!(f, "{}", self.status),
        }
    }
}

impl Response {
    pub(crate) fn new(status: StatusCode, body: ResponseBody) -> Self {
        Self { status, body }
    }
}

// TODO revamp this - should also handle error states with a value
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ResponseBody {
    Get(Option<ResponseBodyGet>),
    Set,
    Delete,
    Flush,
}

impl fmt::Display for ResponseBody {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delete => write!(f, "DELETE"),
            Self::Set => write!(f, "SET"),
            Self::Flush => write!(f, "FLUSH"),
            Self::Get(maybe_get) => match maybe_get {
                None => write!(f, "GET None"),
                Some(get_resp) => write!(f, "{get_resp}"),
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ResponseBodyGet {
    pub key: Key,
    pub value: Value,
    pub ttl_since_unix_epoch_in_millis: Option<u128>,
}

impl fmt::Display for ResponseBodyGet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.ttl_since_unix_epoch_in_millis {
            None => write!(f, "\"{}\"", self.value),
            Some(ttl) => write!(f, "{} EXP {}", self.value, ttl),
        }
    }
}

impl TryFrom<Response> for ResponseFrame {
    type Error = Error;
    fn try_from(resp: Response) -> Result<Self> {
        let (op_code, key, value, ttl) = match resp.body {
            ResponseBody::Get(get_body) => {
                let (k, v, ttl) = get_body.map_or((None, None, None), |b| {
                    (Some(b.key), Some(b.value), b.ttl_since_unix_epoch_in_millis)
                });
                (OpCode::Get, k, v, ttl)
            }
            ResponseBody::Set => (OpCode::Set, None, None, None),
            ResponseBody::Delete => (OpCode::Delete, None, None, None),
            ResponseBody::Flush => (OpCode::Flush, None, None, None),
        };
        let ttl = TTLSinceUnixEpochInMillis::parse(ttl);
        ResponseFrame::new(op_code, resp.status, ttl, key, value)
    }
}

impl TryFrom<ResponseFrame> for Response {
    type Error = Error;

    fn try_from(frame: ResponseFrame) -> Result<Response> {
        let body = match frame.header.op_code {
            OpCode::Get => {
                // TODO beautify
                let body_result = match (frame.key, frame.value) {
                    (Some(key), Some(value)) => {
                        let ttl_since_unix_epoch_in_millis =
                            frame.header.ttl_since_unix_epoch_in_millis.into_ttl();
                        Ok(Some(ResponseBodyGet {
                            key,
                            value,
                            ttl_since_unix_epoch_in_millis,
                        }))
                    }
                    (Some(_), None) => Err(Error::new_parse(ParseError::ValueMissing)),
                    (None, Some(_)) => Err(Error::new_parse(ParseError::KeyMissing)),
                    (None, None) => Err(Error::new_parse(ParseError::KeyAndValueMissing)),
                };
                let body = match body_result {
                    Ok(Some(response_body)) => Some(response_body),
                    Ok(None) => None,
                    Err(e) => {
                        if frame.header.status == StatusCode::Ok {
                            return Err(e);
                        }
                        None
                    }
                };
                ResponseBody::Get(body)
            }
            OpCode::Set => {
                ensure_key_and_value_are_none(frame.key, frame.value)?;
                ResponseBody::Set
            }
            OpCode::Delete => {
                ensure_key_and_value_are_none(frame.key, frame.value)?;
                ResponseBody::Delete
            }
            OpCode::Flush => {
                ensure_key_and_value_are_none(frame.key, frame.value)?;
                ResponseBody::Flush
            }
        };
        Ok(Self {
            status: frame.header.status,
            body,
        })
    }
}

fn ensure_key_and_value_are_none(key: Option<Key>, value: Option<Value>) -> Result<()> {
    if key.is_some() {
        Err(Error::new_parse(ParseError::UnexpectedKey))
    } else if value.is_some() {
        Err(Error::new_parse(ParseError::UnexpectedValue))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::response::{Response, ResponseBody, ResponseBodyGet};
    use rstest::rstest;

    #[rstest]
    #[case(
        OpCode::Get,
        StatusCode::Ok,
        Some("ABC".to_string()),
        Some("Some value".to_string()),
        None,
        ResponseBody::Get(Some( ResponseBodyGet {key: Key::parse("ABC".to_string()).unwrap(), value: Value::parse("Some value".to_string()).unwrap(), ttl_since_unix_epoch_in_millis: None}))
    )]
    #[case(
        OpCode::Get,
        StatusCode::Ok,
        Some("ABC".to_string()),
        Some("Some value".to_string()),
        Some(123456678901),
        ResponseBody::Get(Some( ResponseBodyGet {key: Key::parse("ABC".to_string()).unwrap(), value: Value::parse("Some value".to_string()).unwrap(), ttl_since_unix_epoch_in_millis: Some(123456678901)}))
    )]
    #[case(OpCode::Set, StatusCode::Ok, None, None, None, ResponseBody::Set)]
    #[case(OpCode::Delete, StatusCode::Ok, None, None, None, ResponseBody::Delete)]
    #[case(OpCode::Flush, StatusCode::Ok, None, None, None, ResponseBody::Flush)]
    fn test_conversion_from_valid_response_frame_to_response_works(
        #[case] op_code: OpCode,
        #[case] status: StatusCode,
        #[case] key: Option<String>,
        #[case] value: Option<String>,
        #[case] ttl: Option<u128>,
        #[case] expected_response_body: ResponseBody,
    ) {
        let key = key.map(Key::parse).transpose().unwrap();
        let value = value.map(Value::parse).transpose().unwrap();
        let ttl = TTLSinceUnixEpochInMillis::parse(ttl);
        let resp_frame = ResponseFrame::new(op_code, status, ttl, key, value).unwrap();
        assert_eq!(
            Response::try_from(resp_frame).unwrap(),
            Response {
                status,
                body: expected_response_body
            }
        )
    }

    #[rstest]
    #[case(
        OpCode::Get,
        StatusCode::Ok,
        Some("ABC".to_string()),
        None,
    )]
    #[case(OpCode::Get, StatusCode::Ok, None, None)]
    #[case(OpCode::Set, StatusCode::Ok, Some("ABC".to_string()), None)]
    #[case(OpCode::Set, StatusCode::Ok, None, Some("ABC".to_string()))]
    #[case(OpCode::Set, StatusCode::Ok, Some("ABC".to_string()), Some("ABC".to_string()))]
    #[case(OpCode::Delete, StatusCode::Ok, Some("ABC".to_string()), None)]
    #[case(OpCode::Delete, StatusCode::Ok, None, Some("ABC".to_string()))]
    #[case(OpCode::Delete, StatusCode::Ok, Some("ABC".to_string()), Some("ABC".to_string()))]
    #[case(OpCode::Flush, StatusCode::Ok, Some("ABC".to_string()), None)]
    #[case(OpCode::Flush, StatusCode::Ok, None, Some("ABC".to_string()))]
    #[case(OpCode::Flush, StatusCode::Ok, Some("ABC".to_string()), Some("ABC".to_string()))]
    fn test_conversion_from_invalid_response_frame_to_response_fails(
        #[case] op_code: OpCode,
        #[case] status: StatusCode,
        #[case] key: Option<String>,
        #[case] value: Option<String>,
    ) {
        let key = key.map(Key::parse).transpose().unwrap();
        let value = value.map(Value::parse).transpose().unwrap();
        let ttl = TTLSinceUnixEpochInMillis::parse(None);
        let resp_frame = ResponseFrame::new(op_code, status, ttl, key, value).unwrap();
        assert!(Response::try_from(resp_frame).is_err())
    }
}
