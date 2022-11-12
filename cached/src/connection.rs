use crate::error::{ConnectionError, Error, FrameError, Result};
use crate::frame::header::Header;
use crate::frame::{Frame, RequestFrame, ResponseFrame};
use crate::request::Request;
use crate::response::Response;
use bytes::{Buf, BytesMut};
use std::fmt::Debug;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
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

    #[instrument(skip(self))]
    pub async fn send_request(&mut self, request: Request) -> Result<Response> {
        self.write_request(request).await?;
        match self.read_response().await? {
            Some(response) => Ok(response),
            None => Err(Error::Connection(ConnectionError::Read(
                "Could not read response".to_string(),
            ))),
        }
    }

    #[instrument(skip(self))]
    pub async fn read_request(&mut self) -> Result<Option<Request>> {
        let frame = self.read_frame::<RequestFrame>().await;
        match frame {
            Ok(maybe_frame) => maybe_frame.map(Request::try_from).transpose(),
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self))]
    pub async fn read_response(&mut self) -> Result<Option<Response>> {
        let frame = self.read_frame::<ResponseFrame>().await;
        match frame {
            Ok(maybe_frame) => maybe_frame.map(Response::try_from).transpose(),
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self))]
    pub async fn write_request(&mut self, request: Request) -> Result<()> {
        let frame = RequestFrame::try_from(request)?;
        self.write_frame(&frame).await
    }

    #[instrument(skip(self))]
    pub async fn write_response(&mut self, response: Response) -> Result<()> {
        let frame = ResponseFrame::try_from(response)?;
        self.write_frame(&frame).await
    }

    async fn read_frame<F>(&mut self) -> Result<Option<F>>
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
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(Error::Connection(ConnectionError::ResetByPeer))
                };
            }
        }
    }

    fn parse_frame<F>(&mut self) -> Result<Option<F>>
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

    async fn write_frame<F>(&mut self, frame: &F) -> Result<()>
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
            .write_u8(frame.get_header().get_status_or_padding())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u8(frame.get_header().get_key_length())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u128(
                frame
                    .get_header()
                    .get_ttl_since_unix_epoch_in_millis()
                    .into_inner(),
            )
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
