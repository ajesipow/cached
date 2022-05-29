use crate::{OpCode, RequestFrame};

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum Request {
    Get(String),
    Set { key: String, value: String },
    Delete(String),
    Flush,
}

impl TryFrom<RequestFrame> for Request {
    type Error = String;

    fn try_from(frame: RequestFrame) -> Result<Self, Self::Error> {
        match frame.header.op_code {
            OpCode::Set => Ok(Request::Set {
                key: frame
                    .key
                    .ok_or(())
                    .map_err(|_| "Missing key for Set request.".to_string())?,
                value: frame
                    .value
                    .ok_or(())
                    .map_err(|_| "Missing value for Set request.".to_string())?,
            }),
            OpCode::Get => {
                if frame.value.is_some() {
                    return Err("Must not provide value for Get request.".to_string());
                }
                Ok(Request::Get(
                    frame
                        .key
                        .ok_or(())
                        .map_err(|_| "Missing key for Get request.".to_string())?,
                ))
            }
            OpCode::Delete => {
                if frame.value.is_some() {
                    return Err("Must not provide value for Delete request.".to_string());
                }
                Ok(Request::Delete(frame.key.ok_or(()).map_err(|_| {
                    "Missing key for Delete request.".to_string()
                })?))
            }
            OpCode::Flush => {
                if frame.value.is_some() || frame.key.is_some() {
                    return Err("Must not provide key and/or value for Flush request.".to_string());
                }
                Ok(Request::Flush)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::OpCode;
    use crate::{Request, RequestFrame, RequestHeader};
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
        Request::Set {key: "ABC".to_string(), value: "Some value".to_string() }
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
            header: RequestHeader::new(op_code, key.as_deref(), value.as_deref()),
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
            header: RequestHeader::new(op_code, key.as_deref(), value.as_deref()),
            key,
            value,
        };
        assert!(Request::try_from(req_frame).is_err())
    }
}
