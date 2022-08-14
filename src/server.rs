use crate::primitives::Status;
use crate::request::Request;
use crate::response::{Response, ResponseBody, ResponseBodyGet};
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::{broadcast, mpsc, Semaphore};

use crate::connection::Connection;
use crate::db::{Database, Db};
use crate::error::ConnectionError;
use crate::shutdown::Shutdown;
use crate::{error, Error};
use tracing::{debug, error, info, instrument};

#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    port: u16,
    db: Db,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    connection_limit: Arc<Semaphore>,
}

#[derive(Debug)]
pub struct ServerBuilder<A>
where
    A: ToSocketAddrs,
{
    socket_address: A,
    max_connections: Option<usize>,
}

impl<A: ToSocketAddrs> ServerBuilder<A> {
    pub fn new(addr: A) -> Self {
        Self {
            socket_address: addr,
            max_connections: None,
        }
    }

    /// Controls the maximum number of connections the server have open at any one point.
    pub fn max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = Some(max_connections);
        self
    }

    pub async fn try_build(self) -> error::Result<Server> {
        let listener = TcpListener::bind(self.socket_address)
            .await
            .map_err(|_| Error::Connection(ConnectionError::Bind))?;
        let port = listener
            .local_addr()
            .map_err(|_| Error::Connection(ConnectionError::LocalAddr))?
            .port();
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        Ok(Server {
            listener,
            port,
            db: Db::new(),
            notify_shutdown,
            shutdown_complete_tx,
            shutdown_complete_rx,
            connection_limit: Arc::new(Semaphore::new(self.max_connections.unwrap_or(250))),
        })
    }
}

impl Server {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn builder<A: ToSocketAddrs>(addr: A) -> ServerBuilder<A> {
        ServerBuilder::new(addr)
    }

    pub async fn run(mut self, shutdown: impl Future) {
        tokio::select! {
            res = (&mut self).serve() => {
                if let Err(e) = res {
                    error!("Error: {:?}", e);
                }
            }
            _ = shutdown => {
                info!("Shutting down");
            }
        }

        let Self {
            notify_shutdown,
            shutdown_complete_tx,
            mut shutdown_complete_rx,
            ..
        } = self;

        drop(notify_shutdown);
        drop(shutdown_complete_tx);

        let _ = shutdown_complete_rx.recv().await;
    }

    async fn serve(&mut self) -> crate::error::Result<()> {
        loop {
            self.connection_limit
                .acquire()
                .await
                .map_err(|_| Error::Connection(ConnectionError::AcquireSemaphore))?
                .forget();

            let (stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|_| Error::Connection(ConnectionError::Accept))?;
            let mut handler = Handler {
                conn: Connection::new(stream),
                db: self.db.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                connection_limit: self.connection_limit.clone(),
            };
            tokio::spawn(async move {
                handler.run().await;
            });
        }
    }
}

struct Handler {
    conn: Connection,
    db: Db,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    connection_limit: Arc<Semaphore>,
}

impl Handler {
    async fn run(&mut self) {
        while !self.shutdown.is_shutdown() {
            let request = tokio::select! {
                res = self.conn.read_request() => res.unwrap(),
                _ = self.shutdown.recv() => {
                    debug!("Received shutdown signal.");
                    return
                }
            };
            if let Some(r) = request {
                let response = self.handle_request(r).await;
                self.conn.write_response(response).await.unwrap();
            } else {
                break;
            }
        }
    }

    #[instrument(skip(self))]
    async fn handle_request(&self, req: Request) -> Response {
        match req {
            Request::Get(key) => {
                let response = match self.db.get(&key).await {
                    Some(val) => Response::new(
                        Status::Ok,
                        ResponseBody::Get(Some(ResponseBodyGet {
                            key,
                            value: val.value.to_string(),
                            ttl_since_unix_epoch_in_millis: val.ttl_since_unix_epoch_in_millis,
                        })),
                    ),
                    None => Response::new(Status::KeyNotFound, ResponseBody::Get(None)),
                };
                response
            }
            Request::Set {
                key,
                value,
                ttl_since_unix_epoch_in_millis,
            } => {
                if self.db.contains_key(&key).await {
                    Response::new(Status::KeyExists, ResponseBody::Set)
                } else {
                    self.db
                        .insert(key, value, ttl_since_unix_epoch_in_millis)
                        .await;
                    Response::new(Status::Ok, ResponseBody::Set)
                }
            }
            Request::Delete(key) => {
                if !self.db.contains_key(&key).await {
                    Response::new(Status::KeyNotFound, ResponseBody::Delete)
                } else {
                    self.db.remove(&key).await;
                    Response::new(Status::Ok, ResponseBody::Delete)
                }
            }
            Request::Flush => {
                self.db.clear().await;
                Response::new(Status::Ok, ResponseBody::Flush)
            }
        }
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        self.connection_limit.add_permits(1);
        debug!("Added permit back to connection semaphore.");
    }
}
