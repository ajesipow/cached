use cached::{Client, ClientConnection};
use std::time::Duration;
use tokio::join;
use tracing::info;
use tracing::subscriber::set_global_default;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

#[tokio::main]
async fn main() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let formatting_layer = BunyanFormattingLayer::new("cached_client".into(), std::io::stdout);
    let subscriber = Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer);
    set_global_default(subscriber).expect("Failed to set subscriber");

    let pool = ClientConnection::new("127.0.0.1:7878").await;
    let client = Client::new(pool);

    let key = "Spme key!".to_string();
    let resp = client.get(key.clone()).await;
    info!("Got response: {:?}", resp);

    tokio::time::sleep(Duration::from_secs(10)).await;

    let resp = client
        .set(key.clone(), "My own private value".to_string(), None)
        .await;
    info!("Got response: {:?}", resp);
    let resp = client.get(key.clone()).await;
    info!("Got response: {:?}", resp);

    let resp = client.delete(key.clone()).await;
    info!("Got response: {:?}", resp);

    let resp = client.get(key.clone()).await;
    info!("Got response: {:?}", resp);

    let resp = client
        .set(key.clone(), "My own private value".to_string(), None)
        .await;
    info!("Got response: {:?}", resp);

    let resp = client.get(key.clone()).await;
    info!("Got response: {:?}", resp);

    let resp = client.flush().await;
    info!("Got response: {:?}", resp);

    let res = join!(client.get(key.clone()), client.get(key.clone()));
    info!("Got join response: {:?}", res);
}
