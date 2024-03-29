use cached::{Client, Server, StatusCode};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

async fn run_test_server() -> SocketAddr {
    let host = "127.0.0.1";
    let server = Server::new()
        .max_connections(1)
        .bind(format!("{host}:0"))
        .await
        .unwrap();
    let server_port = server.port();
    tokio::spawn(server.run());
    format!("{host}:{server_port}")
        .parse()
        .expect("Could not parse address as SocketAddr")
}

#[tokio::test]
async fn test_getting_a_non_existing_key_fails() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());
}

#[tokio::test]
async fn test_setting_a_key_works() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());

    let resp = client.set(key.clone(), value.clone(), None).await.unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::Ok);
    assert_eq!(resp.value(), Some(&value));
    assert_eq!(resp.ttl_since_unix_epoch_in_millis(), None);
}

#[tokio::test]
async fn test_setting_a_key_with_ttl_in_the_future_works() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());

    let ttl = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 1000;
    let resp = client
        .set(key.clone(), value.clone(), Some(ttl))
        .await
        .unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::Ok);
    assert_eq!(resp.value(), Some(&value));
    assert_eq!(resp.ttl_since_unix_epoch_in_millis(), Some(ttl));
}

#[tokio::test]
async fn test_setting_a_key_with_ttl_in_the_past_works() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());

    let ttl = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        - 1;
    let resp = client
        .set(key.clone(), value.clone(), Some(ttl))
        .await
        .unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());
}

#[tokio::test]
async fn test_setting_a_key_with_ttl_in_the_future_works_and_then_expires() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());

    let ttl = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 100;
    let resp = client
        .set(key.clone(), value.clone(), Some(ttl))
        .await
        .unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::Ok);
    assert_eq!(resp.value(), Some(&value));
    assert_eq!(resp.ttl_since_unix_epoch_in_millis(), Some(ttl));

    tokio::time::sleep(Duration::from_millis(110)).await;

    let resp = client.get(key).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());
}

#[tokio::test]
async fn test_setting_the_same_key_twice_fails() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());

    let resp = client.set(key.clone(), value.clone(), None).await.unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::Ok);
    assert_eq!(resp.value(), Some(&value));
    assert_eq!(resp.ttl_since_unix_epoch_in_millis(), None);

    let resp = client.set(key, value, None).await.unwrap();
    assert_eq!(resp, StatusCode::KeyExists);
}

#[tokio::test]
async fn test_deleting_a_key_works() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());

    let resp = client.set(key.clone(), value.clone(), None).await.unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::Ok);
    assert_eq!(resp.value(), Some(&value));
    assert_eq!(resp.ttl_since_unix_epoch_in_millis(), None);

    let resp = client.delete(key.clone()).await.unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());
}

#[tokio::test]
async fn test_deleting_a_non_existing_key_fails() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();

    let resp = client.delete(key.clone()).await.unwrap();
    assert_eq!(resp, StatusCode::KeyNotFound);
}

#[tokio::test]
async fn test_flushing_works() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());

    let resp = client.set(key.clone(), value.clone(), None).await.unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::Ok);
    assert_eq!(resp.value(), Some(&value));
    assert_eq!(resp.ttl_since_unix_epoch_in_millis(), None);

    let resp = client.flush().await.unwrap();
    assert_eq!(resp, StatusCode::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());
}

#[tokio::test]
async fn test_setting_and_getting_keys_concurrently_works() {
    let address = run_test_server().await;
    let client = Client::new(address).await;

    let key_1 = "ABC".to_string();
    let key_2 = "DEF".to_string();
    let value_1 = "1234".to_string();
    let value_2 = "5678".to_string();
    let (resp_1, resp_2) = tokio::join!(
        client.set(key_1.clone(), value_1.clone(), None),
        client.set(key_2.clone(), value_2.clone(), None)
    );
    assert_eq!(resp_1.unwrap(), StatusCode::Ok);
    assert_eq!(resp_2.unwrap(), StatusCode::Ok);

    let (resp_1, resp_2) = tokio::join!(client.get(key_1.clone()), client.get(key_2.clone()));
    let resp_1 = resp_1.unwrap();
    let resp_2 = resp_2.unwrap();
    assert_eq!(resp_1.status(), StatusCode::Ok);
    assert_eq!(resp_1.value(), Some(&value_1));
    assert_eq!(resp_1.ttl_since_unix_epoch_in_millis(), None);

    assert_eq!(resp_2.status(), StatusCode::Ok);
    assert_eq!(resp_2.value(), Some(&value_2));
    assert_eq!(resp_2.ttl_since_unix_epoch_in_millis(), None);
}

#[tokio::test]
async fn test_max_connections_limit() {
    let address = run_test_server().await;
    let client_1 = Client::new(address).await;
    let client_2 = Client::new(address).await;

    let key = "ABC".to_string();
    let resp = client_1.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());

    // We're expecting a timeout here as only one connection is allowed
    assert!(
        timeout(Duration::from_millis(100), client_2.get(key.clone()))
            .await
            .is_err()
    );

    // Once client 1 frees up the connection, client 2 should be able to connect
    drop(client_1);

    let resp = client_2.get(key.clone()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::KeyNotFound);
    assert!(resp.value().is_none());
}
