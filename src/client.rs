use crate::connection::Connection;
use crate::error::ConnectionError;
use crate::error::{Error, Result};
use crate::request::Request;
use crate::response::Response;
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
pub struct ClientConnection {
    sender: mpsc::Sender<RequestResponder>,
}

impl ClientConnection {
    /// Panics if cannot connect to addr
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        let (tx, mut rx) = mpsc::channel::<RequestResponder>(32);
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = Connection::new(stream);
        spawn(async move {
            while let Some(request_responder) = rx.recv().await {
                let responder = request_responder.responder;
                let res = conn.send_request(request_responder.request).await;
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
    pub fn new(conn: ClientConnection) -> Self {
        Self { conn: conn.get() }
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
