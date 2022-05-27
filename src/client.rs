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
        let header = RequestHeader::new(OpCode::Get, Some(key.as_str()), None);
        let request_frame = RequestFrame::new(header, Some(key), None);
        self.conn
            .write_frame(&request_frame)
            .await
            .map_err(|_| ())?;
        let frame = loop {
            if let Ok(Some(f)) = self.conn.read_frame::<ResponseFrame>().await {
                break f;
            }
        };
        Response::try_from(frame).map_err(|_| ())
    }
}
