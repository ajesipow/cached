use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub(crate) struct Db {
    request_sender: mpsc::Sender<DbRequestWithResponder>,
}

#[derive(Debug, Clone)]
pub(crate) struct DbValue {
    pub value: String,
    pub ttl_since_unix_epoch_in_millis: Option<u128>,
}

enum DbRequest {
    Get(String),
    Insert {
        key: String,
        value: String,
        ttl: Option<u128>,
    },
    Remove(String),
    ContainsKey(String),
    Clear,
}

enum DbResponse {
    Get(DbValue),
    ContainsKey(bool),
}

struct DbRequestWithResponder {
    request: DbRequest,
    result_channel: oneshot::Sender<Option<DbResponse>>,
}

struct MainDB {
    db: HashMap<String, DbValue>,
    keys_with_ttl: HashSet<String>,
}

impl MainDB {
    fn new() -> Self {
        Self {
            db: HashMap::new(),
            keys_with_ttl: Default::default(),
        }
    }

    fn handle_request(&mut self, request: DbRequest) -> Option<DbResponse> {
        match request {
            DbRequest::Get(key) => self.get(&key).map(DbResponse::Get),
            DbRequest::Insert { key, value, ttl } => {
                self.insert(key, value, ttl);
                None
            }
            DbRequest::ContainsKey(key) => {
                Some(DbResponse::ContainsKey(self.db.contains_key(&key)))
            }
            DbRequest::Remove(key) => {
                self.remove(&key);
                None
            }
            DbRequest::Clear => {
                self.clear();
                None
            }
        }
    }

    fn get(&mut self, key: &str) -> Option<DbValue> {
        let maybe_value = self.db.get(key);
        let maybe_ttl = maybe_value
            .as_ref()
            .and_then(|value| value.ttl_since_unix_epoch_in_millis);

        let ttl_has_expired = maybe_ttl
            .map(|ttl| {
                ttl < SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis()
            })
            .unwrap_or(false);

        if ttl_has_expired {
            self.db.remove(key);
            self.keys_with_ttl.remove(key);
            None
        } else {
            maybe_value.cloned()
        }
    }

    fn insert(&mut self, key: String, value: String, ttl_since_unix_epoch_in_millis: Option<u128>) {
        if let Some(ttl) = ttl_since_unix_epoch_in_millis {
            if ttl
                <= SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis()
            {
                // TTL in the past, don't store anything
                return;
            }
            self.keys_with_ttl.insert(key.clone());
        }
        self.db.insert(
            key,
            DbValue {
                value,
                ttl_since_unix_epoch_in_millis,
            },
        );
    }

    fn remove(&mut self, key: &str) {
        self.db.remove(key);
        self.keys_with_ttl.remove(key);
    }

    fn clear(&mut self) {
        self.db.clear();
        self.keys_with_ttl.clear();
    }
}

impl Db {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::channel::<DbRequestWithResponder>(32);
        let main_db = MainDB::new();
        tokio::spawn(Self::run(rx, main_db));
        Self { request_sender: tx }
    }

    async fn run(mut rx: Receiver<DbRequestWithResponder>, mut main_db: MainDB) {
        while let Some(responder) = rx.recv().await {
            let response = main_db.handle_request(responder.request);
            let result_channel = responder.result_channel;
            let _ = result_channel.send(response);
        }
    }
}

#[async_trait]
pub(crate) trait Database: Clone {
    type Output;

    async fn insert(&self, key: String, value: String, ttl: Option<u128>);

    async fn get(&self, key: &str) -> Option<Self::Output>;

    async fn remove(&self, key: &str);

    async fn contains_key(&self, key: &str) -> bool;

    async fn clear(&self);
}

#[async_trait]
impl Database for Db {
    type Output = DbValue;

    async fn insert(
        &self,
        key: String,
        value: String,
        ttl_since_unix_epoch_in_millis: Option<u128>,
    ) {
        let (tx, _) = oneshot::channel::<Option<DbResponse>>();
        let db_responder = DbRequestWithResponder {
            request: DbRequest::Insert {
                key,
                value,
                ttl: ttl_since_unix_epoch_in_millis,
            },
            result_channel: tx,
        };
        let _ = self.request_sender.send(db_responder).await;
    }

    async fn get(&self, key: &str) -> Option<Self::Output> {
        let (tx, rx) = oneshot::channel::<Option<DbResponse>>();
        let db_responder = DbRequestWithResponder {
            request: DbRequest::Get(key.to_string()),
            result_channel: tx,
        };
        let _ = self.request_sender.send(db_responder).await;
        rx.await.ok().and_then(|v| match v {
            Some(DbResponse::Get(value)) => Some(value),
            _ => None,
        })
    }

    async fn remove(&self, key: &str) {
        let (tx, _) = oneshot::channel::<Option<DbResponse>>();
        let db_responder = DbRequestWithResponder {
            request: DbRequest::Remove(key.to_string()),
            result_channel: tx,
        };
        let _ = self.request_sender.send(db_responder).await;
    }

    async fn contains_key(&self, key: &str) -> bool {
        let (tx, rx) = oneshot::channel::<Option<DbResponse>>();
        let db_responder = DbRequestWithResponder {
            request: DbRequest::ContainsKey(key.to_string()),
            result_channel: tx,
        };
        let _ = self.request_sender.send(db_responder).await;
        rx.await.ok().map_or(false, |v| match v {
            Some(DbResponse::ContainsKey(value)) => value,
            _ => false,
        })
    }

    async fn clear(&self) {
        let (tx, _) = oneshot::channel::<Option<DbResponse>>();
        let db_responder = DbRequestWithResponder {
            request: DbRequest::Clear,
            result_channel: tx,
        };
        let _ = self.request_sender.send(db_responder).await;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_ttl_elapsed_does_not_return_value_from_db() {
        let db = Db::new();
        let key = "Hello";
        let value = "World";
        let valid_until = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1;
        db.insert(key.to_string(), value.to_string(), Some(valid_until))
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;
        // Must not return the key as its TTL expired already
        assert!(db.get(key).await.is_none());
    }

    #[tokio::test]
    async fn test_ttl_elapsed_does_not_return_value_from_main_db() {
        let mut db = MainDB::new();
        let key = "Hello";
        let value = "World";
        let valid_until = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1;
        db.insert(key.to_string(), value.to_string(), Some(valid_until));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());

        tokio::time::sleep(Duration::from_millis(10)).await;
        // Must not return the key as its TTL expired already
        assert!(db.get(key).is_none());

        // Ensure everything is cleaned up
        assert!(db.db.get(key).is_none());
        assert!(db.keys_with_ttl.get(key).is_none());
    }

    #[tokio::test]
    async fn test_ttl_in_past_does_not_store_value() {
        let mut db = MainDB::new();
        let key = "Hello";
        let value = "World";
        let valid_until_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        db.insert(key.to_string(), value.to_string(), Some(valid_until_now));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.db.get(key).is_none());
        assert!(db.keys_with_ttl.get(key).is_none());
    }

    #[tokio::test]
    async fn test_ttl_in_future_returns_value_db() {
        let db = Db::new();
        let key = "Hello";
        let value = "World";
        let valid_until_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1;
        db.insert(key.to_string(), value.to_string(), Some(valid_until_now))
            .await;

        // Must not return the key as its TTL expired already
        assert!(db.get(key).await.is_some());
    }

    #[tokio::test]
    async fn test_ttl_in_future_returns_value_main_db() {
        let mut db = MainDB::new();
        let key = "Hello";
        let value = "World";
        let valid_until_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1;
        db.insert(key.to_string(), value.to_string(), Some(valid_until_now));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());

        // Must not return the key as its TTL expired already
        assert!(db.get(key).is_some());

        // Ensure everything is still present
        assert!(db.db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());
    }

    #[tokio::test]
    async fn test_removing_key_is_also_removed_from_ttl_set_main_db() {
        let mut db = MainDB::new();
        let key = "Hello";
        let value = "World";
        let valid_until_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 100;
        db.insert(key.to_string(), value.to_string(), Some(valid_until_now));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());

        db.remove(key);

        // Ensure everything is removed
        assert!(db.db.get(key).is_none());
        assert!(db.keys_with_ttl.get(key).is_none());
    }

    #[tokio::test]
    async fn test_contains_key_works() {
        let db = Db::new();
        let key = "Hello";
        let value = "World";
        db.insert(key.to_string(), value.to_string(), None).await;

        assert!(db.contains_key(key).await);
        db.remove(key).await;
        assert!(!db.contains_key(key).await);
    }

    #[tokio::test]
    async fn test_clearing_db_works() {
        let db = Db::new();
        let key = "Hello";
        let value = "World";
        db.insert(key.to_string(), value.to_string(), None).await;

        assert!(db.contains_key(key).await);
        db.clear().await;
        assert!(!db.contains_key(key).await);
    }

    #[tokio::test]
    async fn test_clearing_db_works_main_db() {
        let mut db = MainDB::new();
        let key = "Hello";
        let value = "World";
        db.insert(key.to_string(), value.to_string(), None);

        assert!(db.db.contains_key(key));
        db.clear();
        assert_eq!(db.db.len(), 0);
        assert_eq!(db.keys_with_ttl.len(), 0);
    }
}
