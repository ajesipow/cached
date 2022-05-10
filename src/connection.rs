use crate::frame::{Error, Frame};
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum ConnectionError {
    Read(String),
    Parse,
    ResetByPeer,
    Write,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self
                .stream
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
        self.stream.write_u8(frame.header.op_code as u8).await?;
        self.stream.write_u8(frame.header.key_length).await?;
        self.stream
            .write_u32(frame.header.total_frame_length)
            .await?;
        if let Some(key) = frame.key.as_ref() {
            self.stream.write_all(key.as_bytes()).await?;
        }
        if let Some(value) = frame.value.as_ref() {
            self.stream.write_all(value.as_bytes()).await?;
        }

        self.stream.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Frame, OpCode};
    use tokio::io;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn test_client_server_ping_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket_address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut connection = Connection::new(stream);
                    if let Ok(Some(frame)) = connection.read_frame().await {
                        connection.write_frame(&frame).await.unwrap();
                    }
                    Ok::<_, io::Error>(())
                });
            }
        });

        let stream = TcpStream::connect(socket_address.to_string())
            .await
            .unwrap();
        let mut connection = Connection::new(stream);

        let frame = Frame::new(
            OpCode::Set,
            Some("super_key".to_string()),
            Some("mega guter value".to_string()),
        );

        connection.write_frame(&frame).await.unwrap();
        let returned_frame = connection.read_frame().await;
        assert_eq!(returned_frame, Ok(Some(frame)))
    }
}
