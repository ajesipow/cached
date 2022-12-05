use crate::error::{ConnectionError, Error, FrameError, Result};
use crate::frame::header::Header;
use crate::frame::{Frame, RequestFrame, ResponseFrame};
use crate::request::Request;
use crate::response::Response;
use bytes::BytesMut;
use std::fmt::Debug;
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

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
    fn parse_frame<F>(&mut self) -> Result<Option<F>>
    where
        F: Frame + Debug,
    {
        parse_frame(&mut self.buffer)
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
            .write_u8(frame.header().get_op_code() as u8)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u8(frame.header().get_status_or_padding())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u8(frame.header().get_key_length())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u128(
                frame
                    .header()
                    .get_ttl_since_unix_epoch_in_millis()
                    .into_inner(),
            )
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        self.stream
            .write_u32(frame.header().get_total_frame_length())
            .await
            .map_err(|_| Error::Connection(ConnectionError::Write))?;
        if let Some(key) = frame.key() {
            self.stream
                .write_all(key.as_bytes())
                .await
                .map_err(|_| Error::Connection(ConnectionError::Write))?;
        }
        if let Some(value) = frame.value() {
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

#[instrument(skip(buffer))]
fn parse_frame<F>(buffer: &mut BytesMut) -> Result<Option<F>>
where
    F: Frame,
    F: Debug,
{
    let buf = &buffer[..];
    match F::check(buf) {
        Ok(_) => {
            let frame = F::parse(buffer)?;
            Ok(Some(frame))
        }
        Err(Error::Frame(FrameError::Incomplete)) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::header::RequestHeader;
    use crate::primitives::OpCode;
    use bytes::BufMut;

    #[global_allocator]
    static ALLOC: dhat::Alloc = dhat::Alloc;

    #[test]
    #[ignore]
    fn test_parsing_request_frame_works() {
        let _profiler = dhat::Profiler::builder().testing().build();
        let data = "\u{1}\0\u{3}\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\u{1e}ABC1234";
        let mut bytes = BytesMut::with_capacity(30);
        bytes.put_slice(data.as_bytes());
        // Get the baseline for setup
        let stats = dhat::HeapStats::get();
        dhat::assert_eq!(stats.total_blocks, 1);
        dhat::assert_eq!(stats.total_bytes, 30);

        // The actual data we're interested in (subtract the baseline)
        let parsed_frame = parse_frame(&mut bytes);
        let stats = dhat::HeapStats::get();
        dhat::assert_eq!(stats.total_blocks, 4);
        dhat::assert_eq!(stats.total_bytes, 77);

        let expected_frame = RequestFrame {
            header: RequestHeader::parse(OpCode::Set, Some("ABC"), Some("1234"), None).unwrap(),
            key: Some("ABC".to_string()),
            value: Some("1234".to_string()),
        };
        assert_eq!(parsed_frame.unwrap(), Some(expected_frame));
    }
}
