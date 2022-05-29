use crate::{Frame, OpCode, ResponseFrame, ResponseHeader, Status};

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

impl From<Response> for ResponseFrame {
    fn from(resp: Response) -> Self {
        let (op_code, key, value) = match resp.body {
            ResponseBody::Get(get_body) => {
                let (k, v) = get_body.map_or((None, None), |b| (Some(b.key), Some(b.value)));
                (OpCode::Get, k, v)
            }
            ResponseBody::Set => (OpCode::Set, None, None),
            ResponseBody::Delete => (OpCode::Delete, None, None),
            ResponseBody::Flush => (OpCode::Flush, None, None),
        };
        let header = ResponseHeader::new(op_code, resp.status, key.as_deref(), value.as_deref());
        ResponseFrame::new(header, key, value)
    }
}

impl TryFrom<ResponseFrame> for Response {
    type Error = String;

    fn try_from(frame: ResponseFrame) -> Result<Response, Self::Error> {
        let body = match frame.header.op_code {
            OpCode::Get => {
                let body = if let (Some(key), Some(value)) = (frame.key, frame.value) {
                    Some(ResponseBodyGet { key, value })
                } else {
                    if frame.header.status == Status::Ok {
                        return Err(
                            "Expected key and value, but one or both are missing.".to_string()
                        );
                    }
                    None
                };
                ResponseBody::Get(body)
            }
            OpCode::Set => {
                if frame.key.is_some() || frame.value.is_some() {
                    return Err("Must not provide key and value for Set response".to_string());
                }
                ResponseBody::Set
            }
            OpCode::Delete => {
                if frame.key.is_some() || frame.value.is_some() {
                    return Err("Must not provide key and value for Delete response".to_string());
                }
                ResponseBody::Delete
            }
            OpCode::Flush => {
                if frame.key.is_some() || frame.value.is_some() {
                    return Err("Must not provide key and value for Flush response".to_string());
                }
                ResponseBody::Flush
            }
        };
        Ok(Self {
            status: frame.header.status,
            body,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        OpCode, Response, ResponseBody, ResponseBodyGet, ResponseFrame, ResponseHeader, Status,
    };
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
            header: ResponseHeader::new(op_code, status, key.as_deref(), value.as_deref()),
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
            header: ResponseHeader::new(op_code, status, key.as_deref(), value.as_deref()),
            key,
            value,
        };
        assert!(Response::try_from(resp_frame).is_err())
    }
}
