use crate::request::Request;
use crate::response::{ResponseBody, ResponseBodyGet};
use crate::Response;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::io;
use tokio::net::{TcpListener, ToSocketAddrs};

use crate::connection::Connection;
use crate::frame::{RequestFrame, ResponseFrame, Status};
use tracing::instrument;

type Db = Arc<DashMap<String, String>>;

#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    port: u16,
    db: Db,
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
        Ok(Self {
            listener,
            port,
            db: Arc::new(DashMap::with_shard_amount(128)),
        })
    }

    pub async fn serve(self) -> Result<(), io::Error> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let mut handler = Handler {
                conn: Connection::new(stream),
                db: self.db.clone(),
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
}

impl Handler {
    async fn run(&mut self) {
        loop {
            let request = self.read_request().await.unwrap();
            if let Some(r) = request {
                let response = self.handle_request(r);
                self.write_response(response).await.unwrap();
            } else {
                break;
            }
        }
    }

    #[instrument(skip(self))]
    async fn read_request(&mut self) -> crate::error::Result<Option<Request>> {
        let frame = self.conn.read_frame::<RequestFrame>().await;
        match frame {
            Ok(maybe_frame) => maybe_frame.map(Request::try_from).transpose(),
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self))]
    async fn write_response(&mut self, resp: Response) -> crate::error::Result<()> {
        let frame = ResponseFrame::try_from(resp)?;
        self.conn.write_frame(&frame).await
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
