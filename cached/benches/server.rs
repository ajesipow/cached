use cached::{Client, Server};
use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::join_all;
use tokio::time::Instant;

fn get_key(c: &mut Criterion) {
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
        // Seed the server with some data
        let client = Client::new("127.0.0.1:6599").await;
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

fn get_same_key_in_parallel_single_client(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        tokio::spawn(async {
            Server::new()
                .bind("127.0.0.1:6599")
                .await
                .unwrap()
                .run()
                .await;
        });
        // Seed the server with some data
        let client = Client::new("127.0.0.1:6599").await;
        client
            .set("hello".to_string(), "world".to_string(), None)
            .await
            .unwrap();
        drop(client);
    });

    c.bench_function("get bursts single client", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let client = Client::new("127.0.0.1:6599").await;
            let client_futures = (0..iters)
                .into_iter()
                .map(|_| client.get("hello".to_string()));
            let start = Instant::now();
            let responses = join_all(client_futures).await;
            let elapsed = start.elapsed();

            let failed = responses.iter().filter(|resp| resp.is_err()).count();
            if failed > 0 {
                eprintln!("failed {failed} requests (might be bench timeout)");
            };
            elapsed
        })
    });
}

fn get_same_key_in_parallel_multiple_clients(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        tokio::spawn(async {
            Server::new()
                .bind("127.0.0.1:6599")
                .await
                .unwrap()
                .run()
                .await;
        });
        let client = Client::new("127.0.0.1:6599").await;
        client
            .set("hello".to_string(), "world".to_string(), None)
            .await
            .unwrap();
        drop(client);
    });

    c.bench_function("get bursts 100 clients", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let client_futures = (0..100).into_iter().map(|_| Client::new("127.0.0.1:6599"));
            let clients = join_all(client_futures).await;
            let client_futures = (0..iters)
                .into_iter()
                .flat_map(|_| clients.iter().map(|c| c.get("hello".to_string())));
            let start = Instant::now();
            let responses = join_all(client_futures).await;
            let elapsed = start.elapsed();

            let failed = responses.iter().filter(|resp| resp.is_err()).count();
            if failed > 0 {
                eprintln!("failed {failed} requests (might be bench timeout)");
            };
            elapsed
        })
    });
}

criterion_group!(
    benches,
    get_key,
    get_same_key_in_parallel_single_client,
    get_same_key_in_parallel_multiple_clients
);
criterion_main!(benches);
