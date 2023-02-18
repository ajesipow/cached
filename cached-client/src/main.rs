mod input_parsing;

use crate::input_parsing::{convert_error, parse_input, Request};
use cached::Client;
use clap::Parser;
use nom::Err;
use std::io::Write;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    port: u16,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let port = cli.port;
    let server_addr = format!("127.0.0.1:{port}");
    let client = Client::new(&server_addr).await;
    let mut input = String::new();
    loop {
        input.clear();
        print!("{}> ", &server_addr);
        let _ = std::io::stdout().flush();
        std::io::stdin().read_line(&mut input).unwrap();
        let parsed_input = parse_input(&input);
        match parsed_input {
            Ok((_, maybe_request)) => match maybe_request {
                Some(request) => match request {
                    Request::Get(key) => {
                        let res = client.get(key).await;
                        println!("{res:?}");
                    }
                    Request::Set {
                        key,
                        value,
                        ttl_since_unix_epoch_in_millis,
                    } => {
                        let res = client.set(key, value, ttl_since_unix_epoch_in_millis).await;
                        println!("{res:?}");
                    }
                    Request::Delete(key) => {
                        let res = client.delete(key).await;
                        println!("{res:?}");
                    }
                    Request::Flush => {
                        let res = client.flush().await;
                        println!("{res:?}");
                    }
                },
                None => break,
            },
            Err(Err::Failure(f)) => match convert_error(f) {
                Some(context) => {
                    eprintln!("{context}");
                }
                None => eprintln!("Invalid command"),
            },
            _ => {
                eprintln!("Invalid command")
            }
        }
    }
}
