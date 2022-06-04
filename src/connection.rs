use crate::error::{ConnectionError, Error, FrameError, Result};
use crate::frame::Frame;
use crate::Header;
use bytes::{Buf, BytesMut};
use std::fmt::Debug;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
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

    pub async fn read_frame<F>(&mut self) -> Result<Option<F>>
    where
        F: Frame + Debug,
    {
        loop {
            self.stream.get_ref().readable().await.map_err(|_| {
                Error::Connection(ConnectionError::Read(
                    "Could not read from stream".to_string(),
                ))
            })?;
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            if 0 == self
                .stream
                .read_buf(&mut self.buffer)
                .await
                .map_err(|e| Error::Connection(ConnectionError::Read(e.to_string())))?
            {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Error::Connection(ConnectionError::ResetByPeer));
                }
            }
        }
    }

    pub fn parse_frame<F>(&mut self) -> Result<Option<F>>
    where
        F: Frame + Debug,
    {
        let mut buf = Cursor::new(&self.buffer[..]);
        match F::check(&mut buf) {
            Ok(frame_length) => {
                buf.set_position(0);
                let frame = F::parse(&mut buf)?;
                self.buffer.advance(frame_length);
                Ok(Some(frame))
            }
            Err(Error::Frame(FrameError::Incomplete)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn write_frame<F>(&mut self, frame: &F) -> Result<()>
    where
        F: Frame + Debug,
    {
        // TODO error conversion
        self.stream
            .get_ref()
            .writable()
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        // TODO re-implement this elsewhere, the order etc is very specific to frame and should live there probably
        self.stream
            .write_u8(frame.get_header().get_op_code() as u8)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u8(frame.get_header().get_status_or_blank())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u8(frame.get_header().get_key_length())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u32(frame.get_header().get_total_frame_length())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        if let Some(key) = frame.get_key() {
            self.stream
                .write_all(key.as_bytes())
                .await
                .map_err(|_| Error::Connection(ConnectionError::Write))?;
        }
        if let Some(value) = frame.get_value() {
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
