use thiserror::Error;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct Error(#[from] pub(crate) ErrorInner);

#[derive(Error, Debug)]
pub(crate) enum ErrorInner {
    // TODO check if all errors are really needed
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error(transparent)]
    Server(#[from] ServerError),
}

impl Error {
    pub(crate) fn new_parse(e: ParseError) -> Self {
        Self(e.into())
    }

    pub(crate) fn new_frame(e: FrameError) -> Self {
        Self(e.into())
    }

    pub(crate) fn new_connection(e: ConnectionError) -> Self {
        Self(e.into())
    }

    pub(crate) fn new_server(e: ServerError) -> Self {
        Self(e.into())
    }

    pub(crate) fn is_incomplete_frame(&self) -> bool {
        matches!(self, Self(ErrorInner::Frame(FrameError::Incomplete)))
    }
}

#[derive(Error, Debug)]
pub(crate) enum ParseError {
    #[error("key missing")]
    KeyMissing,
    #[error("value missing")]
    ValueMissing,
    #[error("key and value missing")]
    KeyAndValueMissing,
    #[error("unexpected key")]
    UnexpectedKey,
    #[error("unexpected value")]
    UnexpectedValue,
    #[error("key too long")]
    KeyTooLong,
    #[error("value too long")]
    ValueTooLong,
    #[error(transparent)]
    String(#[from] std::string::FromUtf8Error),
    // TODO better nom error
    #[error("could not parse")]
    Other,
}

#[derive(Error, Debug)]
pub(crate) enum FrameError {
    #[error("incomplete")]
    Incomplete,
    #[error("invalid OpCode")]
    InvalidOpCode,
    #[error("invalid StatusCode")]
    InvalidStatusCode,
}

#[derive(Error, Debug)]
pub(crate) enum ConnectionError {
    #[error("could not read response")]
    ReadResponse,
    #[error("connection reset by peer")]
    ResetByPeer,
    #[error("could not write")]
    Write,
    #[error("could not send")]
    Send,
    #[error("could not receive")]
    Receive,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("could not acquire semaphore")]
    AcquireSemaphore,
    #[error("could not bind to address")]
    Bind,
}

#[derive(Error, Debug)]
pub(crate) enum ServerError {
    #[error("no value returned")]
    NoValueReturned,
}
