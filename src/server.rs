use crate::{Connection, Frame, OpCode};
use std::time::Duration;
use tokio::io;
use tokio::net::{TcpListener, ToSocketAddrs};

pub struct Server;

impl Server {
    pub async fn run<A>(addr: A) -> Result<(), io::Error>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let frame = Frame::new(
                    OpCode::Set,
                    Some("super_key".to_string()),
                    Some("mega guter value".to_string()),
                );
                let mut connection = Connection::new(stream);
                for _ in 1..5 {
                    println!("Client add: {:?}", addr);
                    connection.write_frame(&frame).await.unwrap();
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Ok::<_, io::Error>(())
            });
        }
    }
}
