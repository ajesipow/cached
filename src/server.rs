use crate::request::Request;
use crate::response::{ResponseBody, ResponseBodyGet};
use crate::Response;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io;
use tokio::net::{TcpListener, ToSocketAddrs};

use crate::connection::Connection;
use crate::frame::{RequestFrame, ResponseFrame, Status};
use tracing::instrument;

#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    port: u16,
    // TODO use sender receiver model
    db: Arc<Mutex<HashMap<String, String>>>,
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
            db: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn serve(self) -> Result<(), io::Error> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let mut connection = Connection::new(stream);
            let db = self.db.clone();
            tokio::spawn(async move {
                loop {
                    let request = read_request(&mut connection).await.unwrap();
                    if let Some(r) = request {
                        let response = handle_request(r, db.clone());
                        write_response(&mut connection, response).await.unwrap();
                    } else {
                        break;
                    }
                }
            });
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

fn handle_request(req: Request, db: Arc<Mutex<HashMap<String, String>>>) -> Response {
    match req {
        Request::Get(key) => {
            let state = db.lock().unwrap();
            match state.get(&key) {
                Some(val) => Response::new(
                    Status::Ok,
                    ResponseBody::Get(Some(ResponseBodyGet {
                        key,
                        // TODO do we want to clone here? Maybe use bytes instead?
                        value: val.to_string(),
                    })),
                ),
                None => Response::new(Status::KeyNotFound, ResponseBody::Get(None)),
            }
        }
        Request::Set { key, value } => {
            let mut state = db.lock().unwrap();
            if let Entry::Vacant(e) = state.entry(key) {
                e.insert(value);
                Response::new(Status::Ok, ResponseBody::Set)
            } else {
                Response::new(Status::KeyExists, ResponseBody::Set)
            }
        }
        Request::Delete(key) => {
            let mut state = db.lock().unwrap();
            if !state.contains_key(&key) {
                Response::new(Status::KeyNotFound, ResponseBody::Delete)
            } else {
                state.remove(&key);
                Response::new(Status::Ok, ResponseBody::Delete)
            }
        }
        Request::Flush => {
            let mut state = db.lock().unwrap();
            state.clear();
            Response::new(Status::Ok, ResponseBody::Flush)
        }
    }
}
