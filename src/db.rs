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

    fn insert(&self, key: String, value: String, ttl_since_unix_epoch_in_millis: Option<u128>) {
        // TODO: don't even insert keys where TTL is in the past
        if ttl_since_unix_epoch_in_millis.is_some() {
            // TODO let's try not to clone here
            self.keys_with_ttl.insert(key.clone());
        }
        self.main_db.insert(
            key,
            DbValue {
                value,
                ttl_since_unix_epoch_in_millis,
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

    #[tokio::test]
    async fn test_earlier_ttl_does_not_return_value() {
        let db = Db::default();
        let key = "Hello";
        let value = "World";
        let valid_until_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - 1;
        db.insert(key.to_string(), value.to_string(), Some(valid_until_now));

        // Ensure key is in main db and set of keys with TTL
        assert!(db.main_db.get(key).is_some());
        assert!(db.keys_with_ttl.get(key).is_some());

        // Must not return the key as its TTL expired already
        assert!(db.get(key).is_none());

        // Ensure everything is cleaned up
        assert!(db.main_db.get(key).is_none());
        assert!(db.keys_with_ttl.get(key).is_none());
    }
}
