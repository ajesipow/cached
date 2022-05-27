use crate::request::Request;
use crate::response::{ResponseBody, ResponseBodyGet};
use crate::{Connection, RequestFrame, Response, ResponseFrame, Status};
use tokio::io;
use tokio::net::{TcpListener, ToSocketAddrs};

#[derive(Debug)]
pub struct Server(ServerInner);

#[derive(Debug)]
struct ServerInner {
    listener: TcpListener,
}

impl Server {
    pub async fn try_bind<A>(addr: A) -> Result<Server, io::Error>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self(ServerInner { listener }))
    }

    pub async fn serve(self) -> Result<(), io::Error> {
        loop {
            let (stream, _) = self.0.listener.accept().await?;
            tokio::spawn(async move {
                let mut connection = Connection::new(stream);
                let request = read_request(&mut connection).await.unwrap();
                let response = handle_request(request);
                write_response(&mut connection, response).await.unwrap();
            });
        }
    }
}

async fn read_request(conn: &mut Connection) -> Result<Request, ()> {
    println!("reading request");
    let frame = conn.read_frame::<RequestFrame>().await;
    println!("Frame: {:?}", frame);
    match frame {
        Ok(maybe_frame) => {
            // TODO what if frame is none?
            Request::try_from(maybe_frame.unwrap())
        }
        // TODO proper error handling
        Err(_) => Err(()),
    }
}

async fn write_response(conn: &mut Connection, resp: Response) -> Result<(), ()> {
    let frame = ResponseFrame::try_from(resp)?;
    // TODO proper error handling
    conn.write_frame(&frame).await.map_err(|_| ())
}

fn handle_request(req: Request) -> Response {
    match req {
        Request::Get(key) => {
            println!("Got get request {:?}", key);
            Response::new(
                Status::Ok,
                ResponseBody::Get(Some(ResponseBodyGet {
                    key,
                    value: "Some value".to_string(),
                })),
            )
        }
        Request::Set { key: _, value: _ } => Response::new(Status::Ok, ResponseBody::Set),
        Request::Delete(_) => Response::new(Status::Ok, ResponseBody::Delete),
        Request::Flush => Response::new(Status::Ok, ResponseBody::Flush),
    }
}
