//! Per-connection TCP handler.
//!
//! Handles the read/write framing loop for a single client connection.

use std::sync::Arc;

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::broker::handler::handle_request;
use crate::error::Result;
use crate::protocol::codec::RukaCodec;
use crate::protocol::request::Request;
use crate::storage::topic::TopicRegistry;

/// Handles a single client TCP connection.
pub async fn handle_connection(stream: TcpStream, registry: Arc<TopicRegistry>) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    tracing::info!("Client connected: {}", peer_addr);

    // Wrap the raw TCP stream with our length-delimited custom protocol codec
    let mut framed = Framed::new(stream, RukaCodec);

    // Process incoming frames in a loop
    while let Some(frame_result) = framed.next().await {
        match frame_result {
            Ok(frame) => {
                // 1. Decode frame into a typed Request
                let request = match Request::from_frame(frame) {
                    Ok(req) => req,
                    Err(e) => {
                        tracing::error!("Invalid request from {}: {}", peer_addr, e);
                        // We could send an ErrorResponse if we parsed the correlation ID,
                        // but since the frame itself was invalid, we just drop the connection.
                        break;
                    }
                };

                tracing::debug!("Received request from {}: {:?}", peer_addr, request);

                // 2. Route the request to the broker logic (Storage engine)
                let response = handle_request(request, Arc::clone(&registry)).await;

                tracing::debug!("Sending response to {}: {:?}", peer_addr, response);

                // 3. Encode the typed Response back to a frame and send it
                let response_frame = response.into_frame();
                if let Err(e) = framed.send(response_frame).await {
                    tracing::error!("Failed to write response to {}: {}", peer_addr, e);
                    break;
                }
            }
            Err(e) => {
                tracing::error!("Frame decode error from {}: {}", peer_addr, e);
                break;
            }
        }
    }

    tracing::info!("Client disconnected: {}", peer_addr);
    Ok(())
}
