use cached::{Client, Response, ResponseBody, ResponseBodyGet, Server, Status};

#[tokio::test]
async fn test_getting_a_non_existing_key_fails() {
    // TODO choose random port
    let addr = "127.0.0.1:7877";
    tokio::spawn(Server::try_bind(addr).await.unwrap().serve());
    let mut client = Client::new(addr).await;

    let key = "ABC".to_string();
    let resp = client.get(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);
}

#[tokio::test]
async fn test_setting_a_key_works() {
    // TODO choose random port
    let addr = "127.0.0.1:7878";
    tokio::spawn(Server::try_bind(addr).await.unwrap().serve());
    let mut client = Client::new(addr).await;

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
async fn test_deleting_a_key_works() {
    // TODO choose random port
    let addr = "127.0.0.1:7879";
    tokio::spawn(Server::try_bind(addr).await.unwrap().serve());
    let mut client = Client::new(addr).await;

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
    // TODO choose random port
    let addr = "127.0.0.1:7880";
    tokio::spawn(Server::try_bind(addr).await.unwrap().serve());
    let mut client = Client::new(addr).await;

    let key = "ABC".to_string();

    let resp = client.delete(key.clone()).await.unwrap();
    assert_eq!(resp.status, Status::KeyNotFound);
}

#[tokio::test]
async fn test_flushing_works() {
    // TODO choose random port
    let addr = "127.0.0.1:7881";
    tokio::spawn(Server::try_bind(addr).await.unwrap().serve());
    let mut client = Client::new(addr).await;

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
