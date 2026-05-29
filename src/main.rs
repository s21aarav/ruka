use clap::Parser;
use ruka::config::BrokerConfig;
use ruka::network::server::start_server;
use ruka::storage::topic::TopicRegistry;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = BrokerConfig::parse();
    tracing::info!(?config, "Ruka broker starting");

    let registry = TopicRegistry::load(
        &config.data_dir,
        config.segment_max_bytes,
        config.default_partitions,
        config.sync_level,
        config.channel_capacity,
    )
    .await?;
    let registry_arc = Arc::new(registry);

    tracing::info!("Listening on {}:{}", config.host, config.port);

    // Start TCP server. This loops forever.
    start_server(config.clone(), registry_arc).await?;

    Ok(())
}
