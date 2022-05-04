use cached::{Connection, Frame, OpCode};
use std::io;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").await.unwrap();

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
