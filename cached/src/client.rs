use crate::connection::Connection;
use crate::domain::{Key, Value};
use crate::error::{ConnectionError, ServerError};
use crate::error::{Error, Result};
use crate::request::Request;
use crate::response::{Response, ResponseBody, ResponseGet};
use crate::Error::Server;
use crate::StatusCode;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
#[cfg(feature = "tracing")]
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
    // TODO method to set channel size
    /// Panics if cannot connect to addr.
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        let (tx, mut rx) = mpsc::channel::<RequestResponder>(32);
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = Connection::new(stream);
        // TODO when does this shutdown?
        spawn(async move {
            while let Some(request_responder) = rx.recv().await {
                let responder = request_responder.responder;
                let res = conn.send_request(request_responder.request).await;
                let _ = responder.send(res);
            }
        });
        Self { sender: tx }
    }

    pub fn get(&self) -> mpsc::Sender<RequestResponder> {
        self.sender.clone()
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    conn: mpsc::Sender<RequestResponder>,
}

impl Client {
    /// Panics if cannot connect to addr.
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        let conn = ClientConnection::new(addr).await;
        Self::with_connection(&conn)
    }

    pub fn with_connection(conn: &ClientConnection) -> Self {
        Self { conn: conn.get() }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn get(&self, key: String) -> Result<ResponseGet> {
        let key = Key::parse(key)?;
        let request = Request::Get(key);
        let response = self.handle_request(request).await?;
        if let ResponseBody::Get(maybe_value) = response.body {
            let (value, ttl) = maybe_value.map_or((None, None), |value| {
                (
                    Some(value.value.into_inner()),
                    value.ttl_since_unix_epoch_in_millis,
                )
            });
            Ok(ResponseGet::new(response.status, value, ttl))
        } else {
            Err(Server(ServerError::NoValueReturned))
        }
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn set(
        &self,
        key: String,
        value: String,
        ttl_since_unix_epoch_in_millis: Option<u128>,
    ) -> Result<StatusCode> {
        let key = Key::parse(key)?;
        let value = Value::parse(value)?;
        let request = Request::Set {
            key,
            value,
            ttl_since_unix_epoch_in_millis,
        };
        let response = self.handle_request(request).await?;
        Ok(response.status)
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn delete(&self, key: String) -> Result<StatusCode> {
        let key = Key::parse(key)?;
        let request = Request::Delete(key);
        let response = self.handle_request(request).await?;
        Ok(response.status)
    }

    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn flush(&self) -> Result<StatusCode> {
        let request = Request::Flush;
        let response = self.handle_request(request).await?;
        Ok(response.status)
    }

    async fn handle_request(&self, request: Request) -> Result<Response> {
        let (tx, rx) = oneshot::channel();
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
