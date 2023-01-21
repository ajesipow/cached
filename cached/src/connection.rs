use crate::error::{ConnectionError, Error, FrameError, Result};
use crate::frame::{RequestFrame, ResponseFrame};
use crate::parsing::{parse_request_frame, parse_response_frame};
use crate::request::Request;
use crate::response::Response;
use bytes::{Buf, BytesMut};
use nom::AsBytes;
use std::fmt::Debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
#[cfg(feature = "tracing")]
use tracing::instrument;

#[derive(Debug)]
pub(crate) struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn send_request(&mut self, request: Request) -> Result<Response> {
        self.write_request(request).await?;
        match self.read_response().await? {
            Some(response) => Ok(response),
            None => Err(Error::Connection(ConnectionError::Read(
                "Could not read response".to_string(),
            ))),
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn read_request(&mut self) -> Result<Option<Request>> {
        loop {
            self.stream.get_ref().readable().await.map_err(|_| {
                Error::Connection(ConnectionError::Read(
                    "Could not read from stream".to_string(),
                ))
            })?;
            if let Some(request) = read_request(&mut self.buffer)? {
                return Ok(Some(request));
            }
            if 0 == self
                .stream
                .read_buf(&mut self.buffer)
                .await
                .map_err(|e| Error::Connection(ConnectionError::Read(e.to_string())))?
            {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::Connection(ConnectionError::ResetByPeer))
                };
            }
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn read_response(&mut self) -> Result<Option<Response>> {
        loop {
            self.stream.get_ref().readable().await.map_err(|_| {
                Error::Connection(ConnectionError::Read(
                    "Could not read from stream".to_string(),
                ))
            })?;
            if let Some(response) = read_response(&mut self.buffer)? {
                return Ok(Some(response));
            }
            if 0 == self
                .stream
                .read_buf(&mut self.buffer)
                .await
                .map_err(|e| Error::Connection(ConnectionError::Read(e.to_string())))?
            {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::Connection(ConnectionError::ResetByPeer))
                };
            }
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn write_request(&mut self, request: Request) -> Result<()> {
        // TODO do we even need a Frame?
        let frame = RequestFrame::try_from(request)?;
        // TODO error conversion
        self.stream
            .get_ref()
            .writable()
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        // TODO re-implement this elsewhere, the order etc is very specific to frame and should live there probably
        self.stream
            .write_u8(frame.header.op_code as u8)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        // Padding byte
        self.stream
            .write_u8(0)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u8(frame.header.key_length)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u128(frame.header.ttl_since_unix_epoch_in_millis.into_inner())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u32(frame.header.total_frame_length)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        if let Some(key) = frame.key {
            self.stream
                .write_all(key.as_bytes())
                .await
                .map_err(|_| Error::Connection(ConnectionError::Write))?;
        }
        if let Some(value) = frame.value {
            self.stream
                .write_all(value.as_bytes())
                .await
                .map_err(|_| Error::Connection(ConnectionError::Write))?;
        }
        self.stream
            .flush()
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn write_response(&mut self, response: Response) -> Result<()> {
        // TODO do we even need a Frame?
        let frame = ResponseFrame::try_from(response)?;
        // TODO error conversion
        self.stream
            .get_ref()
            .writable()
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        // TODO re-implement this elsewhere, the order etc is very specific to frame and should live there probably
        self.stream
            .write_u8(frame.header.op_code as u8)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u8(frame.header.status as u8)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u8(frame.header.key_length)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u128(frame.header.ttl_since_unix_epoch_in_millis.into_inner())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u32(frame.header.total_frame_length)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        if let Some(key) = frame.key {
            self.stream
                .write_all(key.as_bytes())
                .await
                .map_err(|_| Error::Connection(ConnectionError::Write))?;
        }
        if let Some(value) = frame.value {
            self.stream
                .write_all(value.as_bytes())
                .await
                .map_err(|_| Error::Connection(ConnectionError::Write))?;
        }
        self.stream
            .flush()
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        Ok(())
    }
}

fn read_request(buffer: &mut BytesMut) -> Result<Option<Request>> {
    match parse_request_frame(buffer.as_bytes()) {
        Err(Error::Frame(FrameError::Incomplete)) => Ok(None),
        Ok(request_frame) => {
            buffer.advance(request_frame.header.total_frame_length as usize);
            Request::try_from(request_frame).map(Some)
        }
        Err(e) => Err(e),
    }
}

fn read_response(buffer: &mut BytesMut) -> Result<Option<Response>> {
    match parse_response_frame(buffer.as_bytes()) {
        Err(Error::Frame(FrameError::Incomplete)) => Ok(None),
        Ok(response_frame) => {
            buffer.advance(response_frame.header.total_frame_length as usize);
            Response::try_from(response_frame).map(Some)
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::domain::{Key, TTLSinceUnixEpochInMillis, Value};
    use crate::primitives::OpCode;

    #[global_allocator]
    static ALLOC: dhat::Alloc = dhat::Alloc;

    #[test]
    #[ignore]
    fn test_parsing_request_frame_works() {
        let _profiler = dhat::Profiler::builder().testing().build();
        let data = "\u{1}\0\u{3}\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\u{1e}ABC1234";
        let bytes = data.as_bytes();
        // Get the baseline for setup
        let stats = dhat::HeapStats::get();
        dhat::assert_eq!(stats.total_blocks, 1);
        dhat::assert_eq!(stats.total_bytes, 30);

        // The actual data we're interested in (subtract the baseline)
        let parsed_frame = parse_request_frame(bytes).unwrap();
        let stats = dhat::HeapStats::get();
        dhat::assert_eq!(stats.total_blocks, 4);
        dhat::assert_eq!(stats.total_bytes, 77);

        let key = Key::parse("ABC".to_string()).unwrap();
        let value = Value::parse("1234".to_string()).unwrap();
        let ttl = TTLSinceUnixEpochInMillis::parse(None);
        let expected_frame = RequestFrame::new(OpCode::Set, ttl, Some(key), Some(value));
        assert_eq!(parsed_frame, expected_frame.unwrap());
    }
}
