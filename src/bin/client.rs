use cached::Connection;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let stream = TcpStream::connect("127.0.0.1:7878").await.unwrap();
    let mut connection = Connection::new(stream);
    loop {
        if let Ok(Some(frame)) = connection.read_frame().await {
            println!("Frame: {:?}", frame);
        }
    }
}
