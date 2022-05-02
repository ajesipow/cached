use cached::{Connection, Frame, OpCode};
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").await.unwrap();

    let frame = Frame::new(
        OpCode::Set,
        Some("super_key".to_string()),
        Some("mega guter value".to_string()),
    );
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection = Connection::new(stream);

        connection.write_frame(&frame).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
