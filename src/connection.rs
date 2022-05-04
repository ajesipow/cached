use crate::frame::{Error, Frame};
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

pub struct Connection {
    read_half: OwnedReadHalf,
    write_half: BufWriter<OwnedWriteHalf>,
    buffer: BytesMut,
}

#[derive(Debug)]
pub enum ConnectionError {
    Read(String),
    Parse,
    ResetByPeer,
    Write,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        let (read_half, write_half) = socket.into_split();
        Self {
            read_half,
            write_half: BufWriter::new(write_half),
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self
                .read_half
                .read_buf(&mut self.buffer)
                .await
                .map_err(|e| ConnectionError::Read(e.to_string()))?
            {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(ConnectionError::ResetByPeer);
                }
            }
        }
    }

    pub fn parse_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        let mut buf = Cursor::new(&self.buffer[..]);
        match Frame::check(&mut buf) {
            Ok(frame_length) => {
                buf.set_position(0);
                let frame = Frame::parse(&mut buf).map_err(|_| ConnectionError::Parse)?;
                self.buffer.advance(frame_length);
                Ok(Some(frame))
            }
            Err(Error::Incomplete) => Ok(None),
            Err(_) => Err(ConnectionError::Parse),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), std::io::Error> {
        self.write_half.write_u8(frame.header.op_code as u8).await?;
        self.write_half.write_u8(frame.header.key_length).await?;
        self.write_half
            .write_u32(frame.header.total_frame_length)
            .await?;
        if let Some(key) = frame.key.as_ref() {
            self.write_half.write_all(key.as_bytes()).await?;
        }
        if let Some(value) = frame.value.as_ref() {
            self.write_half.write_all(value.as_bytes()).await?;
        }

        self.write_half.flush().await?;
        Ok(())
    }
}
