use anyhow::Result;

use routix_matcher::logging;
use routix_matcher::{Config, MatchServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logging::init();
    tracing::info!("Starting Routix Server...");

    let server_address =
        std::env::var("SERVER_ADDRESS").unwrap_or_else(|_| "0.0.0.0:50051".to_string());

    let config = Config::new(&server_address);
    let server = MatchServer::new(config);

    if let Err(e) = server.start().await {
        tracing::error!("Server error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
