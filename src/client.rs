use crate::{Connection, Request, RequestFrame, Response, ResponseFrame};
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
        let request = Request::Get(key);
        self.send_request(request).await
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<Response, ()> {
        let request = Request::Set { key, value };
        self.send_request(request).await
    }

    pub async fn delete(&mut self, key: String) -> Result<Response, ()> {
        let request = Request::Delete(key);
        self.send_request(request).await
    }

    pub async fn flush(&mut self) -> Result<Response, ()> {
        let request = Request::Flush;
        self.send_request(request).await
    }

    async fn send_request(&mut self, request: Request) -> Result<Response, ()> {
        let request_frame = RequestFrame::try_from(request).map_err(|_| ())?;
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
