static NO_TTL_INDICATOR: u128 = 0;

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct TTLSinceUnixEpochInMillis(u128);

impl TTLSinceUnixEpochInMillis {
    pub fn parse(ttl: Option<u128>) -> Self {
        ttl.map_or(Self(NO_TTL_INDICATOR), |ttl_since_unix_epoch_in_millis| {
            if ttl_since_unix_epoch_in_millis == NO_TTL_INDICATOR {
                Self(NO_TTL_INDICATOR)
            } else {
                Self(ttl_since_unix_epoch_in_millis)
            }
        })
    }

    pub fn into_inner(self) -> u128 {
        self.0
    }

    pub fn into_ttl(self) -> Option<u128> {
        match self.0 {
            0 => None,
            ttl => Some(ttl),
        }
    }
}
