use cached::{Client, Pool, Response, ResponseBody, ResponseBodyGet, Server, Status};
use std::net::SocketAddr;

async fn run_test_server() -> SocketAddr {
    let host = "127.0.0.1";
    let server = Server::build(format!("{host}:0")).await.unwrap();
    let server_port = server.port();
    tokio::spawn(server.serve());
    format!("{host}:{server_port}")
        .parse()
        .expect("Could not parse address as SocketAddr")
}

#[tokio::test]
async fn test_getting_a_non_existing_key_fails() {
    let address = run_test_server().await;
    let pool = Pool::new(address).await;
    let client = Client::new(pool);

    let key = "ABC".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);
}

#[tokio::test]
async fn test_setting_a_key_works() {
    let address = run_test_server().await;
    let pool = Pool::new(address).await;
    let client = Client::new(pool);

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);

    let resp = client.set(key.clone(), value.clone()).await.unwrap();
    assert_eq!(resp.status, Status::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(
        resp,
        Response::new(
            Status::Ok,
            ResponseBody::Get(Some(ResponseBodyGet { key, value }))
        )
    );
}

#[tokio::test]
async fn test_setting_the_same_key_twice_fails() {
    let address = run_test_server().await;
    let pool = Pool::new(address).await;
    let client = Client::new(pool);

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);

    let resp = client.set(key.clone(), value.clone()).await.unwrap();
    assert_eq!(resp.status, Status::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(
        resp,
        Response::new(
            Status::Ok,
            ResponseBody::Get(Some(ResponseBodyGet {
                key: key.clone(),
                value: value.clone()
            }))
        )
    );

    let resp = client.set(key, value).await.unwrap();
    assert_eq!(resp.status, Status::KeyExists);
}

#[tokio::test]
async fn test_deleting_a_key_works() {
    let address = run_test_server().await;
    let pool = Pool::new(address).await;
    let client = Client::new(pool);

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);

    let resp = client.set(key.clone(), value.clone()).await.unwrap();
    assert_eq!(resp.status, Status::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(
        resp,
        Response::new(
            Status::Ok,
            ResponseBody::Get(Some(ResponseBodyGet {
                key: key.clone(),
                value
            }))
        )
    );

    let resp = client.delete(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);
}

#[tokio::test]
async fn test_deleting_a_non_existing_key_fails() {
    let address = run_test_server().await;
    let pool = Pool::new(address).await;
    let client = Client::new(pool);

    let key = "ABC".to_string();

    let resp = client.delete(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);
}

#[tokio::test]
async fn test_flushing_works() {
    let address = run_test_server().await;
    let pool = Pool::new(address).await;
    let client = Client::new(pool);

    let key = "ABC".to_string();
    let value = "1234".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);

    let resp = client.set(key.clone(), value.clone()).await.unwrap();
    assert_eq!(resp.status, Status::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(
        resp,
        Response::new(
            Status::Ok,
            ResponseBody::Get(Some(ResponseBodyGet {
                key: key.clone(),
                value
            }))
        )
    );

    let resp = client.flush().await.unwrap();
    assert_eq!(resp.status, Status::Ok);

    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);
}

#[tokio::test]
async fn test_setting_and_getting_keys_concurrently_works() {
    let address = run_test_server().await;
    let pool = Pool::new(address).await;
    let client = Client::new(pool);

    let key_1 = "ABC".to_string();
    let key_2 = "DEF".to_string();
    let value_1 = "1234".to_string();
    let value_2 = "5678".to_string();
    let (resp_1, resp_2) = tokio::join!(
        client.set(key_1.clone(), value_1.clone()),
        client.set(key_2.clone(), value_2.clone())
    );
    assert_eq!(resp_1.unwrap().status, Status::Ok);
    assert_eq!(resp_2.unwrap().status, Status::Ok);

    let (resp_1, resp_2) = tokio::join!(client.get(key_1.clone()), client.get(key_2.clone()));
    assert_eq!(
        resp_1.unwrap(),
        Response::new(
            Status::Ok,
            ResponseBody::Get(Some(ResponseBodyGet {
                key: key_1,
                value: value_1
            }))
        )
    );

    assert_eq!(
        resp_2.unwrap(),
        Response::new(
            Status::Ok,
            ResponseBody::Get(Some(ResponseBodyGet {
                key: key_2,
                value: value_2
            }))
        )
    );
}
