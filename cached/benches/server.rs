use cached::{Client, Server};
use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::join_all;
use rand::distributions::{Alphanumeric, DistString, Distribution, Uniform};
use rand::{thread_rng, Rng};
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
            let client_futures = (0..iters).map(|_| client.get("hello".to_string()));
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
            let client_futures = (0..100).map(|_| Client::new("127.0.0.1:6599"));
            let clients = join_all(client_futures).await;
            let client_futures =
                (0..iters).flat_map(|_| clients.iter().map(|c| c.get("hello".to_string())));
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

enum RandomAccessClientSetup<'a> {
    Set { key: &'a str, value: &'a str },
    Get(&'a str),
    Delete(&'a str),
    Flush,
}

async fn random_client_action<'a>(
    client: &Client,
    data: &'a [RandomAccessClientSetup<'a>],
    data_distribution: &Uniform<usize>,
) {
    let mut rng = thread_rng();
    match data[data_distribution.sample(&mut rng)] {
        RandomAccessClientSetup::Set { key, value } => {
            let _ = client.set(key, value, None).await;
        }
        RandomAccessClientSetup::Get(key) => {
            let _ = client.get(key).await;
        }
        RandomAccessClientSetup::Delete(key) => {
            let _ = client.delete(key).await;
        }
        RandomAccessClientSetup::Flush => {
            let _ = client.flush().await;
        }
    };
}

fn set_and_get_random_access(c: &mut Criterion) {
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
        Client::new("127.0.0.1:6599").await
    });

    let mut rng = thread_rng();
    let keys: Vec<String> = (0..10_000)
        .map(|_| {
            let key_len = rng.gen_range(5..=32);
            Alphanumeric.sample_string(&mut rng, key_len)
        })
        .collect();
    let values: Vec<String> = (0..10_000)
        .map(|_| {
            let value_len = rng.gen_range(32..=256);
            Alphanumeric.sample_string(&mut rng, value_len)
        })
        .collect();
    let key_dist = Uniform::from(0..keys.len());
    let value_dist = Uniform::from(0..values.len());
    let data: Vec<RandomAccessClientSetup> = (0..100_000)
        .map(|_| match rng.gen::<f64>() {
            x if x <= 0.4 => {
                let key = &keys[key_dist.sample(&mut rng)];
                let value = &values[value_dist.sample(&mut rng)];
                RandomAccessClientSetup::Set { key, value }
            }
            x if 0.4 < x && x <= 0.8 => {
                let key = &keys[key_dist.sample(&mut rng)];
                RandomAccessClientSetup::Get(key)
            }
            x if 0.8 < x && x <= 0.95 => {
                let key = &keys[key_dist.sample(&mut rng)];
                RandomAccessClientSetup::Delete(key)
            }
            _ => RandomAccessClientSetup::Flush,
        })
        .collect();

    let data_distribution = Uniform::from(0..data.len());

    c.bench_function("set_and_get_random_access", |b| {
        b.to_async(&rt)
            .iter(|| async { random_client_action(&client, &data, &data_distribution).await })
    });
}

criterion_group!(
    benches,
    get_key,
    get_same_key_in_parallel_single_client,
    get_same_key_in_parallel_multiple_clients,
    set_and_get_random_access,
);
criterion_main!(benches);
