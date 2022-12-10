mod input_parsing;

use crate::input_parsing::parse_input;
use cached::Client;
use clap::Parser;
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
    let server_addr = format!("127.0.0.1:{}", port);
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
                Some(request) => match client.send(request).await {
                    Ok(response) => println!("{}", response),
                    Err(e) => println!("{:?}", e),
                },
                None => break,
            },
            Err(_) => eprintln!("Invalid command"),
        }
    }
}
