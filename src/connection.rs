use crate::frame::{Frame, FrameError};
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

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum ConnectionError {
    Read(String),
    Parse,
    ResetByPeer,
    Write,
}

// pub trait Dispatch {
//     type Read;
//     fn read(conn: Connection) -> Result<Self::Read, ()>;
// }
//
// pub struct ServerDispatch;
//
// impl Dispatch for ServerDispatch {
//     type Read = Request;
//
//     fn read(conn: &mut Connection) -> Result<Self::Read, ()> {
//         let a = conn.read_frame::<RequestFrame>()?;
//     }
// }

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(8 * 1024),
        }
    }

    pub async fn read_frame<F>(&mut self) -> Result<Option<F>, ConnectionError>
    where
        F: Frame + Debug,
    {
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

    pub fn parse_frame<F>(&mut self) -> Result<Option<F>, ConnectionError>
    where
        F: Frame + Debug,
    {
        let mut buf = Cursor::new(&self.buffer[..]);
        match F::check(&mut buf) {
            Ok(frame_length) => {
                buf.set_position(0);
                let frame = F::parse(&mut buf).map_err(|_| ConnectionError::Parse)?;
                self.buffer.advance(frame_length);
                Ok(Some(frame))
            }
            Err(FrameError::Incomplete) => Ok(None),
            Err(_) => Err(ConnectionError::Parse),
        }
    }

    pub async fn write_frame<F>(&mut self, frame: &F) -> Result<(), std::io::Error>
    where
        F: Frame + Debug,
    {
        // TODO re-implement this elsewhere, the order etc is very specific to frame and should live there probably
        self.stream
            .write_u8(frame.get_header().get_op_code() as u8)
            .await?;
        self.stream
            .write_u8(frame.get_header().get_status_or_blank())
            .await?;
        self.stream
            .write_u8(frame.get_header().get_key_length())
            .await?;
        self.stream
            .write_u32(frame.get_header().get_total_frame_length())
            .await?;
        if let Some(key) = frame.get_key() {
            self.stream.write_all(key.as_bytes()).await?;
        }
        if let Some(value) = frame.get_value() {
            self.stream.write_all(value.as_bytes()).await?;
        }
        self.stream.flush().await?;
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use crate::{Connection, Frame, OpCode};
//     use std::time::Duration;
//     use tokio::io;
//     use tokio::net::{TcpListener, TcpStream};
//     use tokio::time::timeout;
//
//     #[tokio::test]
//     async fn test_connection_with_frame_ping_pong() {
//         let timeout_duration = Duration::from_millis(100);
//
//         let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
//         let socket_address = listener.local_addr().unwrap();
//         tokio::spawn(async move {
//             loop {
//                 let (stream, _) = listener.accept().await.unwrap();
//                 tokio::spawn(async move {
//                     let mut connection = Connection::new(stream);
//                     if let Ok(Some(frame)) = connection.read_frame().await {
//                         timeout(timeout_duration, connection.write_frame(&frame))
//                             .await
//                             .unwrap()
//                             .unwrap();
//                     }
//                     Ok::<_, io::Error>(())
//                 });
//             }
//         });
//
//         let stream = TcpStream::connect(socket_address).await.unwrap();
//         let mut connection = Connection::new(stream);
//
//         let frame = Frame::new(
//             OpCode::Set,
//             Some("super_key".to_string()),
//             Some("mega guter value".to_string()),
//         );
//
//         timeout(timeout_duration, connection.write_frame(&frame))
//             .await
//             .unwrap()
//             .unwrap();
//         let returned_frame = timeout(timeout_duration, connection.read_frame())
//             .await
//             .unwrap();
//         assert_eq!(returned_frame, Ok(Some(frame)))
//     }
// }
