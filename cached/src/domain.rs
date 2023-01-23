use crate::error::FrameError;
use crate::error::Result;
use crate::Error;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

static NO_TTL_INDICATOR: u128 = 0;
/// Value must not be greater than 1MB
static MAX_VALUE_LENGTH: u32 = 1024 * 1024;

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
// A value of 0 means no TTL
pub(crate) struct TTLSinceUnixEpochInMillis(u128);

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Value(String);

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Key(String);

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Value {
    pub(crate) fn parse(v: String) -> Result<Self> {
        if v.len() > MAX_VALUE_LENGTH as usize {
            return Err(Error::Frame(FrameError::ValueTooLong));
        }
        Ok(Self(v))
    }

    pub(crate) fn into_inner(self) -> String {
        self.0
    }

    pub(crate) fn len(&self) -> u32 {
        // Guaranteed to not overflow because of MAX_VALUE_LENGTH used in `Self::parse`
        self.0.len() as u32
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Key {
    pub(crate) fn parse(k: String) -> Result<Self> {
        // Key must not be longer than u8::MAX
        if k.len() > u8::MAX as usize {
            return Err(Error::Frame(FrameError::KeyTooLong));
        }
        Ok(Self(k))
    }

    pub(crate) fn into_inner(self) -> String {
        self.0
    }

    pub(crate) fn len(&self) -> u8 {
        // Guaranteed to not overflow because of u8::MAX used in `Self::parse`
        self.0.len() as u8
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Deref for Key {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for Value {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TTLSinceUnixEpochInMillis {
    pub(crate) fn parse(ttl: Option<u128>) -> Self {
        ttl.map_or(Self(NO_TTL_INDICATOR), |ttl_since_unix_epoch_in_millis| {
            if ttl_since_unix_epoch_in_millis == NO_TTL_INDICATOR {
                Self(NO_TTL_INDICATOR)
            } else {
                Self(ttl_since_unix_epoch_in_millis)
            }
        })
    }

    pub(crate) fn into_inner(self) -> u128 {
        self.0
    }

    pub(crate) fn into_ttl(self) -> Option<u128> {
        match self.0 {
            0 => None,
            ttl => Some(ttl),
        }
    }
}
