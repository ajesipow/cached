// #![deny(missing_docs)]
#![deny(missing_debug_implementations)]
// #![cfg_attr(test, deny(rust_2018_idioms))]
// #![cfg_attr(all(test, feature = "full"), deny(unreachable_pub))]
// #![cfg_attr(all(test, feature = "full"), deny(warnings))]
// #![cfg_attr(all(test, feature = "nightly"), feature(test))]
// #![cfg_attr(docsrs, feature(doc_cfg))]

mod client;
mod connection;
mod error;
mod frame;
mod request;
mod response;
mod server;

pub use client::Client;
pub use connection::*;
pub use frame::*;
pub use request::Request;
pub use response::Response;
pub use response::ResponseBody;
pub use response::ResponseBodyGet;
pub use server::Server;
