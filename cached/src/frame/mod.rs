use bytes::{Buf, Bytes, BytesMut};
use std::fmt::Debug;
#[cfg(feature = "tracing")]
use tracing::instrument;

use crate::error::{Error, FrameError, Result};

pub(crate) mod header;
use header::*;

pub(crate) trait Frame {
    type Header: Header + TryFrom<Bytes, Error = Error> + Debug;

    fn new(header: Self::Header, key: Option<String>, value: Option<String>) -> Self;
    fn header(&self) -> &Self::Header;
    fn key(&self) -> Option<&str>;
    fn value(&self) -> Option<&str>;
    fn total_frame_length(&self) -> u32 {
        self.header().get_total_frame_length()
    }

    #[cfg_attr(feature = "tracing", instrument(skip(src)))]
    fn check(src: &[u8]) -> Result<usize> {
        if src.len() < HEADER_SIZE_BYTES as usize {
            return Err(Error::Frame(FrameError::Incomplete));
        }
        let total_frame_length = u32::from_be_bytes(src[19..23].try_into().unwrap()) as usize;
        if src.len() < total_frame_length {
            return Err(Error::Frame(FrameError::Incomplete));
        }
        Ok(total_frame_length)
    }

    #[cfg_attr(feature = "tracing", instrument(skip(src)))]
    fn parse(src: &mut BytesMut) -> Result<Self>
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

    fn header(&self) -> &Self::Header {
        &self.header
    }

    fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn value(&self) -> Option<&str> {
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
    fn header(&self) -> &Self::Header {
        &self.header
    }

    fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }
}

#[cfg_attr(feature = "tracing", instrument(skip(src)))]
fn get_string(src: &mut BytesMut, len: u32) -> Result<String> {
    if src.remaining() < len as usize {
        return Err(Error::Frame(FrameError::Incomplete));
    }
    let value = String::from_utf8_lossy(src.split_to(len as usize).chunk()).to_string();
    Ok(value)
}
