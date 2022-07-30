use dashmap::mapref::one::Ref;
use dashmap::{DashMap, DashSet};
use std::ops::Deref;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Default)]
pub(crate) struct Db {
    main_db: MainDb,
    keys_with_ttl: DashSet<String>,
}

type MainDb = DashMap<String, DbValue>;

#[derive(Debug)]
pub(crate) struct DbValue {
    pub value: String,
    pub ttl_since_unix_epoch_in_millis: Option<u128>,
}

impl Db {
    pub fn new(shard_amount: usize) -> Self {
        // TODO capacity
        Self {
            main_db: DashMap::with_shard_amount(shard_amount),
            keys_with_ttl: Default::default(),
        }
    }
}

pub(crate) trait Database<'a> {
    type Output: Deref<Target = DbValue>;

    fn insert(&self, key: String, value: String, ttl: Option<u128>);

    fn get(&'a self, key: &str) -> Option<Self::Output>;

    fn remove(&self, key: &str);

    fn contains_key(&self, key: &str) -> bool;

    fn clear(&self);
}

impl<'a> Database<'a> for Db {
    type Output = Ref<'a, String, DbValue>;

    fn insert(
        &self,
        key: String,
        value: String,
        maybe_ttl_since_unix_epoch_in_millis: Option<u128>,
    ) {
        if let Some(ttl_since_unix_epoch_in_millis) = maybe_ttl_since_unix_epoch_in_millis {
            if ttl_since_unix_epoch_in_millis
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
        self.main_db.insert(
            key,
            DbValue {
                value,
                ttl_since_unix_epoch_in_millis: maybe_ttl_since_unix_epoch_in_millis,
            },
        );
    }

    fn get(&'a self, key: &str) -> Option<Self::Output> {
        let maybe_value = self.main_db.get(key);
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
            // Need to drop the reference first, otherwise we'll run into a deadlock when removing
            drop(maybe_value);
            self.remove(key);
            None
        } else {
            maybe_value
        }
    }

    fn remove(&self, key: &str) {
        self.main_db.remove(key);
        self.keys_with_ttl.remove(key);
    }

    fn contains_key(&self, key: &str) -> bool {
        self.main_db.contains_key(key)
    }

    fn clear(&self) {
        self.main_db.clear()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_ttl_elapsed_does_not_return_value() {
        let db = Db::default();
        let key = "Hello";
        let value = "World";
        let valid_until = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1;
        db.insert(key.to_string(), value.to_string(), Some(valid_until));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.main_db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());

        tokio::time::sleep(Duration::from_millis(10)).await;
        // Must not return the key as its TTL expired already
        assert!(db.get(key).is_none());

        // Ensure everything is cleaned up
        assert!(db.main_db.get(key).is_none());
        assert!(db.keys_with_ttl.get(key).is_none());
    }

    #[tokio::test]
    async fn test_ttl_in_past_does_not_store_value() {
        let db = Db::default();
        let key = "Hello";
        let value = "World";
        let valid_until_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        db.insert(key.to_string(), value.to_string(), Some(valid_until_now));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.main_db.get(key).is_none());
        assert!(db.keys_with_ttl.get(key).is_none());
    }

    #[tokio::test]
    async fn test_ttl_in_future_returns_value() {
        let db = Db::default();
        let key = "Hello";
        let value = "World";
        let valid_until_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1;
        db.insert(key.to_string(), value.to_string(), Some(valid_until_now));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.main_db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());

        // Must not return the key as its TTL expired already
        assert!(db.get(key).is_some());

        // Ensure everything is still present
        assert!(db.main_db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());
    }

    #[tokio::test]
    async fn test_removing_key_is_also_removed_from_ttl_set() {
        let db = Db::default();
        let key = "Hello";
        let value = "World";
        let valid_until_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 1;
        db.insert(key.to_string(), value.to_string(), Some(valid_until_now));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.main_db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());

        db.remove(key);

        // Ensure everything is removed
        assert!(db.main_db.get(key).is_none());
        assert!(db.keys_with_ttl.get(key).is_none());
    }

    #[tokio::test]
    async fn test_contains_key_works() {
        let db = Db::default();
        let key = "Hello";
        let value = "World";
        db.insert(key.to_string(), value.to_string(), None);

        assert!(db.contains_key(key));
        db.remove(key);
        assert!(!db.contains_key(key));
    }

    #[tokio::test]
    async fn test_clearing_db_works() {
        let db = Db::default();
        let key = "Hello";
        let value = "World";
        db.insert(key.to_string(), value.to_string(), None);

        assert!(db.contains_key(key));
        db.clear();
        assert_eq!(db.main_db.len(), 0);
        assert_eq!(db.keys_with_ttl.len(), 0);
    }
}
