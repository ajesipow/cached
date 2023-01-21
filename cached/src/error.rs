pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum Error {
    Parse(ParseError),
    Frame(FrameError),
    Connection(ConnectionError),
    Server(ServerError),
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum ParseError {
    KeyMissing,
    ValueMissing,
    KeyAndValueMissing,
    String,
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

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum ServerError {
    NoValueReturned,
}
