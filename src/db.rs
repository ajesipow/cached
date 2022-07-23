use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use std::ops::Deref;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub(crate) struct DbValue {
    pub value: String,
    pub ttl_since_unix_epoch_in_millis: Option<u128>,
}

#[derive(Debug)]
pub(crate) struct Db(DbInner);

type DbInner = DashMap<String, DbValue>;

impl Db {
    pub fn new(shard_amount: usize) -> Self {
        Self(DashMap::with_shard_amount(shard_amount))
    }
}

impl Default for Db {
    fn default() -> Self {
        Self(DashMap::with_shard_amount(1))
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
        self.0.insert(
            key,
            DbValue {
                value,
                ttl_since_unix_epoch_in_millis,
            },
        );
        // TODO if value has ttl spawn thread for inserting key into expire_keys_set
    }

    fn get(&'a self, key: &str) -> Option<Self::Output> {
        let maybe_value = self.0.get(key);
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
            // TODO spawn thread and remove value
            None
        } else {
            maybe_value
        }
    }

    fn remove(&self, key: &str) {
        self.0.remove(key);
        // TODO: remove key from expire_keys_set
    }

    fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    fn clear(&self) {
        self.0.clear()
    }
}
