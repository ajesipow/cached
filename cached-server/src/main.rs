use cached::Server;
use clap::Parser;
#[cfg(feature = "tracing")]
use tracing_chrome::ChromeLayerBuilder;
#[cfg(feature = "tracing")]
use tracing_subscriber::prelude::*;

const BANNER: &str = r#"
 ______     ______     ______     __  __     ______     _____
/\  ___\   /\  __ \   /\  ___\   /\ \_\ \   /\  ___\   /\  __-.
\ \ \____  \ \  __ \  \ \ \____  \ \  __ \  \ \  __\   \ \ \/\ \
 \ \_____\  \ \_\ \_\  \ \_____\  \ \_\ \_\  \ \_____\  \ \____-
  \/_____/   \/_/\/_/   \/_____/   \/_/\/_/   \/_____/   \/____/

"#;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    host: String,
    #[arg(short, long)]
    port: u16,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    // Clear terminal output and position the cursor at row 1, column 1
    print!("{esc}[2J{esc}[1;1H", esc = 27 as char);
    println!("{}", BANNER);

    #[cfg(feature = "tracing")]
    {
        println!("Tracing enabled");
        let (chrome_layer, _guard) = ChromeLayerBuilder::new().build();
        tracing_subscriber::registry().with(chrome_layer).init();
    }

    let host = cli.host;
    let addr = format!("{}:{}", host, cli.port);
    let server = Server::new().bind(addr).await.unwrap();
    println!("Cached server running on {}:{}", host, server.port());
    server.run().await;
}
