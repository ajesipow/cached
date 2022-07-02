use crate::error::{Error, Parse, Result};
use crate::frame::{Frame, ResponseFrame, ResponseHeader};
use crate::primitives::{OpCode, Status};

#[derive(Debug, Eq, PartialEq)]
pub struct Response {
    pub status: Status,
    pub body: ResponseBody,
}

impl Response {
    pub fn new(status: Status, body: ResponseBody) -> Self {
        Self { status, body }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ResponseBody {
    Get(Option<ResponseBodyGet>),
    Set,
    Delete,
    Flush,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ResponseBodyGet {
    pub key: String,
    pub value: String,
}

impl TryFrom<Response> for ResponseFrame {
    type Error = Error;
    fn try_from(resp: Response) -> Result<Self> {
        let (op_code, key, value) = match resp.body {
            ResponseBody::Get(get_body) => {
                let (k, v) = get_body.map_or((None, None), |b| (Some(b.key), Some(b.value)));
                (OpCode::Get, k, v)
            }
            ResponseBody::Set => (OpCode::Set, None, None),
            ResponseBody::Delete => (OpCode::Delete, None, None),
            ResponseBody::Flush => (OpCode::Flush, None, None),
        };
        let header = ResponseHeader::parse(op_code, resp.status, key.as_deref(), value.as_deref())?;
        Ok(ResponseFrame::new(header, key, value))
    }
}

impl TryFrom<ResponseFrame> for Response {
    type Error = Error;

    fn try_from(frame: ResponseFrame) -> Result<Response> {
        let body = match frame.header.get_opcode() {
            OpCode::Get => {
                // TODO beautify
                let body_result = match (frame.key, frame.value) {
                    (Some(key), Some(value)) => Ok(Some(ResponseBodyGet { key, value })),
                    (Some(_), None) => Err(Error::Parse(Parse::ValueMissing)),
                    (None, Some(_)) => Err(Error::Parse(Parse::KeyMissing)),
                    (None, None) => Err(Error::Parse(Parse::KeyAndValueMissing)),
                };
                let body = match body_result {
                    Ok(Some(response_body)) => Some(response_body),
                    Ok(None) => None,
                    Err(e) => {
                        if frame.header.get_status() == &Status::Ok {
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
            status: *frame.header.get_status(),
            body,
        })
    }
}

fn ensure_key_and_value_are_none(key: Option<String>, value: Option<String>) -> Result<()> {
    if key.is_some() {
        Err(Error::Parse(Parse::UnexpectedKey))
    } else if value.is_some() {
        Err(Error::Parse(Parse::UnexpectedValue))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Response, ResponseBody, ResponseBodyGet};
    use rstest::rstest;

    #[rstest]
    #[case(
        OpCode::Get,
        Status::Ok,
        Some("ABC".to_string()),
        Some("Some value".to_string()),
        ResponseBody::Get(Some( ResponseBodyGet {key: "ABC".to_string(), value: "Some value".to_string()}))
    )]
    #[case(OpCode::Set, Status::Ok, None, None, ResponseBody::Set)]
    #[case(OpCode::Delete, Status::Ok, None, None, ResponseBody::Delete)]
    #[case(OpCode::Flush, Status::Ok, None, None, ResponseBody::Flush)]
    fn test_conversion_from_valid_response_frame_to_response_works(
        #[case] op_code: OpCode,
        #[case] status: Status,
        #[case] key: Option<String>,
        #[case] value: Option<String>,
        #[case] expected_response_body: ResponseBody,
    ) {
        let resp_frame = ResponseFrame {
            header: ResponseHeader::parse(op_code, status, key.as_deref(), value.as_deref())
                .unwrap(),
            key,
            value,
        };
        assert_eq!(
            Response::try_from(resp_frame),
            Ok(Response {
                status,
                body: expected_response_body
            })
        )
    }

    #[rstest]
    #[case(
        OpCode::Get,
        Status::Ok,
        Some("ABC".to_string()),
        None,
    )]
    #[case(OpCode::Get, Status::Ok, None, None)]
    #[case(OpCode::Set, Status::Ok, Some("ABC".to_string()), None)]
    #[case(OpCode::Set, Status::Ok, None, Some("ABC".to_string()))]
    #[case(OpCode::Set, Status::Ok, Some("ABC".to_string()), Some("ABC".to_string()))]
    #[case(OpCode::Delete, Status::Ok, Some("ABC".to_string()), None)]
    #[case(OpCode::Delete, Status::Ok, None, Some("ABC".to_string()))]
    #[case(OpCode::Delete, Status::Ok, Some("ABC".to_string()), Some("ABC".to_string()))]
    #[case(OpCode::Flush, Status::Ok, Some("ABC".to_string()), None)]
    #[case(OpCode::Flush, Status::Ok, None, Some("ABC".to_string()))]
    #[case(OpCode::Flush, Status::Ok, Some("ABC".to_string()), Some("ABC".to_string()))]
    fn test_conversion_from_invalid_response_frame_to_response_fails(
        #[case] op_code: OpCode,
        #[case] status: Status,
        #[case] key: Option<String>,
        #[case] value: Option<String>,
    ) {
        let resp_frame = ResponseFrame {
            header: ResponseHeader::parse(op_code, status, key.as_deref(), value.as_deref())
                .unwrap(),
            key,
            value,
        };
        assert!(Response::try_from(resp_frame).is_err())
    }
}
