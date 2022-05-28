use cached::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::new("127.0.0.1:7878").await;

    let key = "Spme key!".to_string();
    let resp = client.get(key.clone()).await;
    println!("Got response: {:?}", resp);

    let resp = client
        .set(key.clone(), "My own private value".to_string())
        .await;
    println!("Got response: {:?}", resp);
    let resp = client.get(key.clone()).await;
    println!("Got response: {:?}", resp);

    let resp = client.delete(key.clone()).await;
    println!("Got response: {:?}", resp);

    let resp = client.get(key.clone()).await;
    println!("Got response: {:?}", resp);

    let resp = client
        .set(key.clone(), "My own private value".to_string())
        .await;
    println!("Got response: {:?}", resp);

    let resp = client.get(key.clone()).await;
    println!("Got response: {:?}", resp);

    let resp = client.flush().await;
    println!("Got response: {:?}", resp);

    let resp = client.get(key.clone()).await;
    println!("Got response: {:?}", resp);
}
