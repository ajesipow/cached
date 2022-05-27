use cached::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::new("127.0.0.1:7878").await;
    let resp = client.get("Spme key!".to_string()).await.unwrap();
    println!("Got response: {:?}", resp);
}
