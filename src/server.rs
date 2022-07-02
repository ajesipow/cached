use crate::request::Request;
use crate::response::{ResponseBody, ResponseBodyGet};
use crate::{Response, Status};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::io;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::{broadcast, mpsc};

use crate::connection::Connection;
use crate::frame::{RequestFrame, ResponseFrame};
use crate::shutdown::Shutdown;
use tracing::{debug, error, info, instrument};

type Db = Arc<DashMap<String, String>>;

#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    port: u16,
    db: Db,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
}

impl Server {
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Server {
    pub async fn build<A>(addr: A) -> Result<Server, io::Error>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        Ok(Self {
            listener,
            port,
            db: Arc::new(DashMap::with_shard_amount(128)),
            notify_shutdown,
            shutdown_complete_tx,
            shutdown_complete_rx,
        })
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

    async fn serve(&mut self) -> Result<(), io::Error> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let mut handler = Handler {
                conn: Connection::new(stream),
                db: self.db.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
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
}

impl Handler {
    async fn run(&mut self) {
        while !self.shutdown.is_shutdown() {
            let request = tokio::select! {
                res = read_request(&mut self.conn) => res.unwrap(),
                _ = self.shutdown.recv() => {
                    debug!("Received shutdown signal.");
                    return
                }
            };
            if let Some(r) = request {
                let response = self.handle_request(r);
                write_response(&mut self.conn, response).await.unwrap();
            } else {
                break;
            }
        }
    }

    #[instrument(skip(self))]
    fn handle_request(&self, req: Request) -> Response {
        match req {
            Request::Get(key) => {
                let response = match self.db.get(&key) {
                    Some(val) => Response::new(
                        Status::Ok,
                        ResponseBody::Get(Some(ResponseBodyGet {
                            key,
                            // TODO do we want to clone here? Maybe use bytes instead?
                            value: val.to_string(),
                        })),
                    ),
                    None => Response::new(Status::KeyNotFound, ResponseBody::Get(None)),
                };
                response
            }
            Request::Set { key, value } => {
                let response = if let Entry::Vacant(e) = self.db.entry(key) {
                    e.insert(value);
                    Response::new(Status::Ok, ResponseBody::Set)
                } else {
                    Response::new(Status::KeyExists, ResponseBody::Set)
                };
                response
            }
            Request::Delete(key) => {
                if !self.db.contains_key(&key) {
                    Response::new(Status::KeyNotFound, ResponseBody::Delete)
                } else {
                    self.db.remove(&key);
                    Response::new(Status::Ok, ResponseBody::Delete)
                }
            }
            Request::Flush => {
                self.db.clear();
                Response::new(Status::Ok, ResponseBody::Flush)
            }
        }
    }
}

#[instrument(skip(conn))]
async fn read_request(conn: &mut Connection) -> crate::error::Result<Option<Request>> {
    let frame = conn.read_frame::<RequestFrame>().await;
    match frame {
        Ok(maybe_frame) => maybe_frame.map(Request::try_from).transpose(),
        Err(e) => Err(e),
    }
}

#[instrument(skip(conn))]
async fn write_response(conn: &mut Connection, resp: Response) -> crate::error::Result<()> {
    let frame = ResponseFrame::try_from(resp)?;
    conn.write_frame(&frame).await
}
