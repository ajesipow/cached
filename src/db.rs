use dashmap::mapref::one::Ref;
use dashmap::{DashMap, DashSet};
use std::ops::Deref;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub(crate) struct DbValue {
    pub value: String,
    pub ttl_since_unix_epoch_in_millis: Option<u128>,
}

#[derive(Debug, Clone)]
pub(crate) struct Db {
    main_db: Arc<DbInner>,
    _keys_with_ttl: Arc<DashSet<String>>,
}

type DbInner = DashMap<String, DbValue>;

impl Db {
    pub fn new(shard_amount: usize) -> Self {
        // TODO capacity
        Self {
            main_db: Arc::new(DashMap::with_shard_amount(shard_amount)),
            _keys_with_ttl: Arc::new(Default::default()),
        }
    }
}

impl Default for Db {
    fn default() -> Self {
        Self {
            main_db: Arc::new(DashMap::with_shard_amount(1)),
            _keys_with_ttl: Arc::new(Default::default()),
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
        self.main_db.insert(
            key,
            DbValue {
                value,
                ttl_since_unix_epoch_in_millis,
            },
        );
        // TODO if value has ttl spawn thread for inserting key into expire_keys_set
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
            // TODO spawn thread and remove value
            None
        } else {
            maybe_value
        }
    }

    fn remove(&self, key: &str) {
        self.main_db.remove(key);
        // TODO: remove key from expire_keys_set
    }

    fn contains_key(&self, key: &str) -> bool {
        self.main_db.contains_key(key)
    }

    fn clear(&self) {
        self.main_db.clear()
    }
}
