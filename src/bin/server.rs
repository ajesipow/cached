use cached::Server;

#[tokio::main]
async fn main() {
    Server::run("127.0.0.1:7878").await.unwrap();
}
