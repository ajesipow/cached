use bytes::{Buf, Bytes};
use std::fmt::Debug;
use std::io::Cursor;

use crate::error::{Error, FrameError, Result};
use crate::header::{Header, RequestHeader, ResponseHeader, HEADER_SIZE_BYTES};

pub trait Frame {
    type Header: Header + TryFrom<Bytes, Error = Error> + Debug;

    fn new(header: Self::Header, key: Option<String>, value: Option<String>) -> Self;
    fn get_header(&self) -> &Self::Header;
    fn get_key(&self) -> Option<&str>;
    fn get_value(&self) -> Option<&str>;

    fn check(src: &mut Cursor<&[u8]>) -> Result<usize> {
        if src.remaining() < HEADER_SIZE_BYTES as usize {
            return Err(Error::Frame(FrameError::Incomplete));
        }
        let header = Self::Header::try_from(src.copy_to_bytes(HEADER_SIZE_BYTES as usize))?;

        if src.remaining() < header.get_total_frame_length() as usize - HEADER_SIZE_BYTES as usize {
            return Err(Error::Frame(FrameError::Incomplete));
        }
        Ok(header.get_total_frame_length() as usize)
    }

    fn parse(src: &mut Cursor<&[u8]>) -> Result<Self>
    where
        Self: Sized,
    {
        let header = Self::Header::try_from(src.copy_to_bytes(HEADER_SIZE_BYTES as usize))?;
        let key_length = header.get_key_length();
        let value_length =
            header.get_total_frame_length() - HEADER_SIZE_BYTES as u32 - key_length as u32;
        let key = match key_length {
            0 => None,
            key_length => Some(get_string(src, key_length as u32)?),
        };
        let value = match value_length {
            0 => None,
            value_length => Some(get_string(src, value_length)?),
        };
        Ok(Self::new(header, key, value))
    }
}

#[derive(Debug)]
pub struct ResponseFrame {
    pub header: ResponseHeader,
    pub key: Option<String>,
    pub value: Option<String>,
}

impl Frame for ResponseFrame {
    type Header = ResponseHeader;

    fn new(header: Self::Header, key: Option<String>, value: Option<String>) -> Self {
        Self { header, key, value }
    }

    fn get_header(&self) -> &Self::Header {
        &self.header
    }

    fn get_key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn get_value(&self) -> Option<&str> {
        self.value.as_deref()
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct RequestFrame {
    pub header: RequestHeader,
    pub key: Option<String>,
    pub value: Option<String>,
}

impl Frame for RequestFrame {
    type Header = RequestHeader;

    fn new(header: Self::Header, key: Option<String>, value: Option<String>) -> Self {
        Self { header, key, value }
    }
    fn get_header(&self) -> &Self::Header {
        &self.header
    }

    fn get_key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn get_value(&self) -> Option<&str> {
        self.value.as_deref()
    }
}

fn get_string(src: &mut Cursor<&[u8]>, len: u32) -> Result<String> {
    if src.remaining() < len as usize {
        return Err(Error::Frame(FrameError::Incomplete));
    }
    let value = String::from_utf8_lossy(src.take(len as usize).chunk()).to_string();
    let new_position = src.position() + len as u64;
    src.set_position(new_position);
    Ok(value)
}
