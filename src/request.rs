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
    // TODO define error
    type Error = ();

    fn try_from(frame: RequestFrame) -> Result<Self, Self::Error> {
        match frame.header.op_code {
            OpCode::Set => Ok(Request::Set {
                key: frame.key.ok_or(())?,
                value: frame.value.ok_or(())?,
            }),
            OpCode::Get => Ok(Request::Get(frame.key.ok_or(())?)),
            OpCode::Delete => Ok(Request::Delete(frame.key.ok_or(())?)),
            OpCode::Flush => Ok(Request::Flush),
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
    #[case(
        OpCode::Flush,
        None,
        None,
        Request::Flush
    )]
    fn test_conversion_from_request_frame_to_request_works(
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
}
