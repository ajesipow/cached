use crate::connection::Connection;
use crate::domain::{Key, Value};
use crate::error::{ClientError, ConnectionError};
use crate::error::{Error, Result};
use crate::request::Request;
use crate::response::{Response, ResponseBody, ResponseGet};
use crate::StatusCode;
use std::fmt::Debug;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
#[cfg(feature = "tracing")]
use tracing::instrument;

#[derive(Debug)]
struct RequestResponder {
    request: Request,
    responder: oneshot::Sender<Result<Response>>,
}

/// A  connection
#[derive(Debug, Clone)]
pub struct ClientConnection {
    sender: mpsc::Sender<RequestResponder>,
}

impl ClientConnection {
    // TODO method to set channel size
    /// Create a new client connection.
    ///
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

    fn get(&self) -> mpsc::Sender<RequestResponder> {
        self.sender.clone()
    }
}

/// A client to communicate with the cached server.
#[derive(Debug, Clone)]
pub struct Client {
    conn: mpsc::Sender<RequestResponder>,
}

impl Client {
    /// Create a new client connecting to a server at `addr`.
    ///
    /// Panics if it cannot connect to addr.
    ///
    /// # Examples
    ///
    /// ```
    /// use cached::Client;
    /// # use cached::Server;
    /// # use cached::Error;
    /// use cached::StatusCode;
    /// use cached::ClientConnection;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # let server = Server::new().bind("127.0.0.1:0").await.unwrap();
    /// # let port = server.port();
    /// # tokio::spawn(async { server.run().await;});
    /// let client = Client::new(format!("127.0.0.1:{port}")).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        let conn = ClientConnection::new(addr).await;
        Self::with_connection(&conn)
    }

    /// Creates a new client using an existing connection.
    ///
    /// This is useful for creating multiple clients that communicate with the server
    /// via the same TCP connection.
    ///
    /// # Examples
    ///
    /// ```
    /// use cached::Client;
    /// # use cached::Server;
    /// # use cached::Error;
    /// use cached::StatusCode;
    /// use cached::ClientConnection;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # let server = Server::new().bind("127.0.0.1:0").await.unwrap();
    /// # let port = server.port();
    /// # tokio::spawn(async { server.run().await;});
    /// let conn = ClientConnection::new(format!("127.0.0.1:{port}")).await;
    ///
    /// let client_1 = Client::with_connection(&conn);
    /// let response = client_1.set("foo", "bar", None).await.unwrap();
    /// assert_eq!(response, StatusCode::Ok);
    ///
    /// let client_2 = Client::with_connection(&conn);
    /// let response = client_2.get("foo").await.unwrap();
    /// assert_eq!(response.status(), StatusCode::Ok);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_connection(conn: &ClientConnection) -> Self {
        Self { conn: conn.get() }
    }

    /// Gets a value by its key from the server.
    ///
    /// # Examples
    ///
    /// ```
    /// use cached::Client;
    /// # use cached::Server;
    /// # use cached::Error;
    /// use cached::StatusCode;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # let server = Server::new().bind("127.0.0.1:0").await.unwrap();
    /// # let port = server.port();
    /// # tokio::spawn(async { server.run().await;});
    /// let client = Client::new(format!("127.0.0.1:{port}")).await;
    /// client.set("foo", "bar", None).await.unwrap();
    ///
    /// let response = client.get("foo").await.unwrap();
    /// assert_eq!(response.status(), StatusCode::Ok);
    /// assert_eq!(response.value().unwrap(), "bar");
    /// assert!(response.ttl_since_unix_epoch_in_millis().is_none());
    ///
    /// let response = client.get("something else").await.unwrap();
    /// assert_eq!(response.status(), StatusCode::KeyNotFound);
    /// assert!(response.value().is_none());
    /// assert!(response.ttl_since_unix_epoch_in_millis().is_none());
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn get<S>(&self, key: S) -> Result<ResponseGet>
    where
        S: Into<String>,
        S: Debug,
    {
        let key = Key::parse(key.into())?;
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
            Err(Error::new_client(ClientError::ExpectedValue))
        }
    }

    /// Sets a value for the given key with an optional expiry time.
    /// Existing values for the key are not overwritten.
    ///
    /// The expiry time must be set as Unix epoch in milliseconds.
    /// The server will not return a value for expired keys.
    ///
    /// # Examples
    ///
    /// ```
    /// use cached::Client;
    /// # use cached::Server;
    /// # use cached::Error;
    /// use cached::StatusCode;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # let server = Server::new().bind("127.0.0.1:0").await.unwrap();
    /// # let port = server.port();
    /// # tokio::spawn(async { server.run().await;});
    /// let client = Client::new(format!("127.0.0.1:{port}")).await;
    /// let response = client.set("foo", "bar", None).await;
    /// assert_eq!(response.unwrap(), StatusCode::Ok);
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn set<S>(
        &self,
        key: S,
        value: S,
        ttl_since_unix_epoch_in_millis: Option<u128>,
    ) -> Result<StatusCode>
    where
        S: Into<String>,
        S: Debug,
    {
        let key = Key::parse(key.into())?;
        let value = Value::parse(value.into())?;
        let request = Request::Set {
            key,
            value,
            ttl_since_unix_epoch_in_millis,
        };
        let response = self.handle_request(request).await?;
        Ok(response.status)
    }

    /// Deletes a key with its value from the cache.
    ///
    /// # Examples
    ///
    /// ```
    /// use cached::Client;
    /// # use cached::Server;
    /// # use cached::Error;
    /// use cached::StatusCode;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # let server = Server::new().bind("127.0.0.1:0").await.unwrap();
    /// # let port = server.port();
    /// # tokio::spawn(async { server.run().await;});
    /// let client = Client::new(format!("127.0.0.1:{port}")).await;
    /// client.set("foo", "bar", None).await.unwrap();
    ///
    /// let response = client.get("foo").await.unwrap();
    /// assert_eq!(response.status(), StatusCode::Ok);
    ///
    /// let response = client.delete("foo").await.unwrap();
    /// assert_eq!(response, StatusCode::Ok);
    ///
    /// let response = client.get("foo").await.unwrap();
    /// assert_eq!(response.status(), StatusCode::KeyNotFound);
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "tracing", instrument(skip(self)))]
    pub async fn delete<S>(&self, key: S) -> Result<StatusCode>
    where
        S: Into<String>,
        S: Debug,
    {
        let key = Key::parse(key.into())?;
        let request = Request::Delete(key);
        let response = self.handle_request(request).await?;
        Ok(response.status)
    }

    /// Clears the entire cache.
    ///
    /// # Examples
    ///
    /// ```
    /// use cached::Client;
    /// # use cached::Server;
    /// # use cached::Error;
    /// use cached::StatusCode;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # let server = Server::new().bind("127.0.0.1:0").await.unwrap();
    /// # let port = server.port();
    /// # tokio::spawn(async { server.run().await;});
    /// let client = Client::new(format!("127.0.0.1:{port}")).await;
    /// client.set("foo", "bar", None).await?;
    ///
    /// let response = client.get("foo").await?;
    /// assert_eq!(response.status(), StatusCode::Ok);
    ///
    /// let response = client.flush().await?;
    /// assert_eq!(response, StatusCode::Ok);
    ///
    /// let response = client.get("foo").await?;
    /// assert_eq!(response.status(), StatusCode::KeyNotFound);
    /// # Ok(())
    /// # }
    /// ```
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
            .map_err(|_| Error::new_connection(ConnectionError::Send))?;
        rx.await
            .map_err(|_| Error::new_connection(ConnectionError::Receive))?
    }
}
