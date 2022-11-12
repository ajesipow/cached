use cached::{Client, ClientConnection, Server};
use criterion::{criterion_group, criterion_main, Criterion};

pub fn get_key(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let client = rt.block_on(async {
        tokio::spawn(async {
            Server::new()
                .bind("127.0.0.1:6599")
                .await
                .unwrap()
                .run()
                .await;
        });
        let client_connection = ClientConnection::new("127.0.0.1:6599").await;
        let client = Client::new(&client_connection);
        client
            .set("hello".to_string(), "world".to_string(), None)
            .await
            .unwrap();
        client
    });

    c.bench_function("get", |b| {
        b.to_async(&rt)
            .iter(|| async { client.get("hello".to_string()).await.unwrap() })
    });
}
//
// pub fn get_same_key_in_parallel(c: &mut Criterion) {
//     let rt = tokio::runtime::Builder::new_multi_thread()
//         .enable_all()
//         .build()
//         .unwrap();
//
//     let client = rt.block_on(async {
//         tokio::spawn(async {
//             Server::new()
//                 .bind("127.0.0.1:6599")
//                 .await
//                 .unwrap()
//                 .run()
//                 .await;
//         });
//         let client_connection = ClientConnection::new("127.0.0.1:6599").await;
//         let client = Client::new(&client_connection);
//         client
//             .set("hello".to_string(), "world".to_string(), None)
//             .await
//             .unwrap();
//         let clients =
//         client
//     });
//
//     c.bench_function("get", |b| {
//         b.to_async(&rt)
//             .iter(|| async { client.get("hello".to_string()).await.unwrap() })
//     });
// }

criterion_group!(benches, get_key);
criterion_main!(benches);
