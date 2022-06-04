use crate::{Connection, Frame, OpCode, RequestFrame, RequestHeader, Response, ResponseFrame};
use tokio::net::{TcpStream, ToSocketAddrs};

#[derive(Debug)]
pub struct Client {
    conn: Connection,
}

impl Client {
    /// Creates a new client and panics if it cannot connect.
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let conn = Connection::new(stream);
        Self { conn }
    }

    // TODO: error handling
    pub async fn get(&mut self, key: String) -> Result<Response, ()> {
        // ⁄TODO build a request instead?
        let header = RequestHeader::new(OpCode::Get, Some(key.as_str()), None);
        let request_frame = RequestFrame::new(header, Some(key), None);
        // TODO put this in a dedicated method
        self.conn
            .write_frame(&request_frame)
            .await
            .map_err(|_| ())?;
        match self
            .conn
            .read_frame::<ResponseFrame>()
            .await
            .map_err(|_| ())?
        {
            Some(frame) => Response::try_from(frame).map_err(|_| ()),
            None => Err(()),
        }
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<Response, ()> {
        let header = RequestHeader::new(OpCode::Set, Some(key.as_str()), Some(value.as_str()));
        let request_frame = RequestFrame::new(header, Some(key), Some(value));
        self.conn
            .write_frame(&request_frame)
            .await
            .map_err(|_| ())?;
        match self
            .conn
            .read_frame::<ResponseFrame>()
            .await
            .map_err(|_| ())?
        {
            Some(frame) => Response::try_from(frame).map_err(|_| ()),
            None => Err(()),
        }
    }

    pub async fn delete(&mut self, key: String) -> Result<Response, ()> {
        let header = RequestHeader::new(OpCode::Delete, Some(key.as_str()), None);
        let request_frame = RequestFrame::new(header, Some(key), None);
        self.conn
            .write_frame(&request_frame)
            .await
            .map_err(|_| ())?;
        match self
            .conn
            .read_frame::<ResponseFrame>()
            .await
            .map_err(|_| ())?
        {
            Some(frame) => Response::try_from(frame).map_err(|_| ()),
            None => Err(()),
        }
    }

    pub async fn flush(&mut self) -> Result<Response, ()> {
        let header = RequestHeader::new(OpCode::Flush, None, None);
        let request_frame = RequestFrame::new(header, None, None);
        self.conn
            .write_frame(&request_frame)
            .await
            .map_err(|_| ())?;
        match self
            .conn
            .read_frame::<ResponseFrame>()
            .await
            .map_err(|_| ())?
        {
            Some(frame) => Response::try_from(frame).map_err(|_| ()),
            None => Err(()),
        }
    }
}
