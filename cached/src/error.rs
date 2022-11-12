pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum Error {
    Parse(Parse),
    Frame(FrameError),
    Connection(ConnectionError),
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum Parse {
    KeyMissing,
    ValueMissing,
    KeyAndValueMissing,
    UnexpectedKey,
    UnexpectedValue,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum FrameError {
    Incomplete,
    InvalidOpCode,
    InvalidStatusCode,
    KeyTooLong,
    ValueTooLong,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum ConnectionError {
    Read(String),
    ResetByPeer,
    Write,
    Send,
    Receive,
    Accept,
    AcquireSemaphore,
    Bind,
    LocalAddr,
    NoConnection(String),
}
