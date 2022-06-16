use crate::connection::Connection;
use crate::error::ConnectionError;
pub use crate::error::{Error, Result};
use crate::frame::{RequestFrame, ResponseFrame};
use crate::{Request, Response};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::instrument;

#[derive(Debug)]
pub struct RequestResponder {
    request: Request,
    responder: oneshot::Sender<Result<Response>>,
}

#[derive(Debug, Clone)]
pub struct Pool {
    sender: mpsc::Sender<RequestResponder>,
}

impl Pool {
    // TODO error handling
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        let (tx, mut rx) = mpsc::channel::<RequestResponder>(32);
        let mut client = ConnectionHandler::new(addr).await;
        spawn(async move {
            while let Some(request_responder) = rx.recv().await {
                let responder = request_responder.responder;
                let res = client.send_request(request_responder.request).await;
                let _ = responder.send(res);
            }
        });
        Self { sender: tx }
    }

    pub fn get(self) -> mpsc::Sender<RequestResponder> {
        self.sender
    }
}

#[derive(Debug)]
pub struct Client {
    conn: mpsc::Sender<RequestResponder>,
}

impl Client {
    pub fn new(pool: Pool) -> Self {
        Self { conn: pool.get() }
    }

    #[instrument(skip(self))]
    pub async fn get(&self, key: String) -> Result<Response> {
        let request = Request::Get(key);
        self.handle_request(request).await
    }

    #[instrument(skip(self))]
    pub async fn set(&self, key: String, value: String) -> Result<Response> {
        let request = Request::Set { key, value };
        self.handle_request(request).await
    }

    #[instrument(skip(self))]
    pub async fn delete(&self, key: String) -> Result<Response> {
        let request = Request::Delete(key);
        self.handle_request(request).await
    }

    #[instrument(skip(self))]
    pub async fn flush(&self) -> Result<Response> {
        let request = Request::Flush;
        self.handle_request(request).await
    }

    async fn handle_request(&self, request: Request) -> Result<Response> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.conn
            .send(RequestResponder {
                request,
                responder: tx,
            })
            .await
            .map_err(|_| Error::Connection(ConnectionError::Send))?;
        rx.await
            .map_err(|_| Error::Connection(ConnectionError::Receive))?
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionHandler {
    conn: Connection,
}

impl ConnectionHandler {
    /// Creates a new client and panics if it cannot connect.
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        let conn = Connection::new(stream);
        Self { conn }
    }

    #[instrument(skip(self))]
    pub async fn send_request(&mut self, request: Request) -> Result<Response> {
        let request_frame = RequestFrame::try_from(request)?;
        self.conn.write_frame(&request_frame).await?;
        match self.conn.read_frame::<ResponseFrame>().await? {
            Some(frame) => Response::try_from(frame),
            None => Err(Error::Connection(ConnectionError::Read(
                "Could not read response".to_string(),
            ))),
        }
    }
}
