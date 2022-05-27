use crate::{OpCode, RequestFrame};

#[derive(Debug)]
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
