//! TCP server loop for accepting incoming client connections.

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::config::BrokerConfig;
use crate::error::Result;
use crate::network::connection::handle_connection;
use crate::storage::topic::TopicRegistry;

/// Start the TCP server loop.
///
/// Binds to the configured address and port, and continuously accepts incoming connections,
/// spawning a new asynchronous Tokio task for each client.
pub async fn start_server(config: BrokerConfig, registry: Arc<TopicRegistry>) -> Result<()> {
    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&bind_addr).await?;

    tracing::info!("Ruka TCP server listening on {}", bind_addr);

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        tracing::debug!("Accepted TCP connection from {}", peer_addr);

                        if let Err(e) = stream.set_nodelay(true) {
                            tracing::warn!("Failed to set TCP_NODELAY on {}: {}", peer_addr, e);
                        }

                        // Clone the Arc to the TopicRegistry so the new task can access it
                        let registry_clone = Arc::clone(&registry);

                        // Spawn a new independent Tokio task to handle this connection concurrently
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, registry_clone).await {
                                tracing::error!("Connection error with {}: {}", peer_addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {}", e);
                        // In a production system, we might want to sleep briefly here
                        // if we hit file descriptor limits, but for now we just continue.
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Shutdown signal received, closing server loop...");
                break;
            }
        }
    }

    Ok(())
}
