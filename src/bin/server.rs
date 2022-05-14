use cached::Server;

#[tokio::main]
async fn main() {
    Server::try_bind("127.0.0.1:7878")
        .await
        .unwrap()
        .serve()
        .await
        .unwrap();
}
