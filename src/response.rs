use crate::{Frame, OpCode, ResponseFrame, ResponseHeader, Status};

#[derive(Debug)]
pub struct Response {
    pub status: Status,
    pub body: ResponseBody,
}

impl Response {
    pub fn new(status: Status, body: ResponseBody) -> Self {
        Self { status, body }
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    Get(Option<ResponseBodyGet>),
    Set,
    Delete,
    Flush,
}

#[derive(Debug)]
pub struct ResponseBodyGet {
    pub key: String,
    pub value: String,
}

impl TryFrom<Response> for ResponseFrame {
    // TODO define error
    type Error = ();

    fn try_from(resp: Response) -> Result<Self, Self::Error> {
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
        Ok(ResponseFrame::new(header, key, value))
    }
}

impl TryFrom<ResponseFrame> for Response {
    type Error = ();

    // TODO: error handling
    fn try_from(value: ResponseFrame) -> Result<Response, Self::Error> {
        let body = match value.header.op_code {
            OpCode::Get => {
                let body = if let (Some(key), Some(value)) = (value.key, value.value) {
                    Some(ResponseBodyGet { key, value })
                } else {
                    None
                };
                ResponseBody::Get(body)
            }
            OpCode::Set => ResponseBody::Set,
            OpCode::Delete => ResponseBody::Delete,
            OpCode::Flush => ResponseBody::Flush,
        };
        Ok(Self {
            status: value.header.status,
            body,
        })
    }
}
