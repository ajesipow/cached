#![allow(dead_code)]
// SET
// - Req: K, V
// - Resp: K, V
// GET
// - Req: K
// - Resp: K, V
// DELETE
// - Req: K
// - Resp: K, V
// FLUSH
// - Req: na
// - Resp: count

use bytes::Buf;
use std::io::Cursor;

pub enum Error {
    Incomplete,
    InvalidOpCode,
}

static HEADER_SIZE_BYTES: u8 = 6;

#[derive(Debug)]
pub struct Frame {
    pub header: Header,
    pub key: Option<String>,
    pub value: Option<String>,
}

impl Frame {
    pub fn new(op_code: OpCode, key: Option<String>, value: Option<String>) -> Self {
        // TODO string must not be longer than what the u8 can hold
        let key_length = key.as_ref().map_or(0, |s| s.len());
        // TODO string must not be longer than what the u32 can hold
        let total_frame_length = HEADER_SIZE_BYTES as u32
            + key_length as u32
            + value.as_ref().map_or(0, |s| s.len() as u32);

        Self {
            header: Header {
                op_code,
                key_length: key_length as u8,
                total_frame_length,
            },
            key,
            value,
        }
    }

    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        let header = Header::try_from(src.take(HEADER_SIZE_BYTES as usize).into_inner())?;
        let payload_length = header.total_frame_length - HEADER_SIZE_BYTES as u32;
        if src.remaining() < payload_length as usize {
            return Err(Error::Incomplete);
        }
        Ok(())
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Self, Error> {
        let header = Header::try_from(src.take(HEADER_SIZE_BYTES as usize).into_inner())?;
        let key_length = header.key_length;
        let value_length = header.total_frame_length - HEADER_SIZE_BYTES as u32 - key_length as u32;
        let key = match key_length {
            0 => None,
            key_length => Some(get_string(src, key_length as u32)?),
        };
        let value = match value_length {
            0 => None,
            value_length => Some(get_string(src, value_length)?),
        };
        Ok(Self { header, key, value })
    }
}

fn get_string(src: &mut Cursor<&[u8]>, len: u32) -> Result<String, Error> {
    if src.remaining() < len as usize {
        return Err(Error::Incomplete);
    }
    let value = String::from_utf8_lossy(src.take(len as usize).chunk()).to_string();
    let new_position = src.position() + len as u64;
    src.set_position(new_position);
    Ok(value)
}

#[derive(Debug)]
pub struct Header {
    pub op_code: OpCode,
    pub key_length: u8,
    pub total_frame_length: u32,
}

impl TryFrom<&mut Cursor<&[u8]>> for Header {
    type Error = Error;

    fn try_from(value: &mut Cursor<&[u8]>) -> Result<Self, Self::Error> {
        if value.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(Error::Incomplete);
        }
        let op_code = OpCode::try_from(value.get_u8())?;
        let key_length = value.get_u8();
        let total_frame_length = value.get_u32();

        Ok(Self {
            op_code,
            key_length,
            total_frame_length,
        })
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum OpCode {
    Set = 1,
    Get = 2,
    Delete = 3,
    Flush = 4,
}

impl TryFrom<u8> for OpCode {
    type Error = Error;

    // TODO: make tests
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(OpCode::Set),
            2 => Ok(OpCode::Get),
            3 => Ok(OpCode::Delete),
            4 => Ok(OpCode::Flush),
            _ => Err(Error::InvalidOpCode),
        }
    }
}

#[derive(Debug)]
#[repr(u8)]
pub enum Status {
    Ok = 1,
    KeyNotFound = 2,
    KeyExists = 3,
    InternalError = 4,
}
