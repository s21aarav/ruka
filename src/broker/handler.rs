//! Request routing and broker logic.
//!
//! Bridges the parsed wire-protocol requests from the network layer
//! to the persistent storage layer.

use std::sync::Arc;

use crate::broker::writer::BrokerCommand;
use tokio::sync::oneshot;

use crate::protocol::request::{FetchRequest, ProduceBatchRequest, ProduceRequest, Request};
use crate::protocol::response::{
    ErrorResponse, FetchResponse, ProduceBatchResponse, ProduceResponse, Response,
};
use crate::protocol::types::ErrorCode;
use crate::storage::topic::TopicRegistry;

/// Handle a parsed client request and return an appropriate response.
pub async fn handle_request(req: Request, registry: Arc<TopicRegistry>) -> Response {
    match req {
        Request::Produce(produce) => handle_produce(produce, registry).await,
        Request::ProduceBatch(batch) => handle_produce_batch(batch, registry).await,
        Request::Fetch(fetch) => handle_fetch(fetch, registry).await,
    }
}

async fn handle_produce(req: ProduceRequest, registry: Arc<TopicRegistry>) -> Response {
    let tx = match registry
        .get_or_create_partition(&req.topic, req.partition)
        .await
    {
        Ok(tx) => tx,
        Err(e) => return ErrorResponse::from_error(req.correlation_id, ErrorCode::from(&e), e),
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    if let Err(e) = tx
        .send(BrokerCommand::Produce {
            key: None,
            value: Some(req.payload),
            reply: reply_tx,
        })
        .await
    {
        return ErrorResponse::from_error(
            req.correlation_id,
            ErrorCode::ServerError,
            format!("Writer task died: {}", e),
        );
    }

    let offset = match reply_rx.await {
        Ok(Ok(offset)) => offset,
        Ok(Err(e)) => return ErrorResponse::from_error(req.correlation_id, ErrorCode::from(&e), e),
        Err(e) => {
            return ErrorResponse::from_error(
                req.correlation_id,
                ErrorCode::ServerError,
                format!("Failed to receive reply: {}", e),
            )
        }
    };

    Response::Produce(ProduceResponse {
        correlation_id: req.correlation_id,
        topic: req.topic,
        partition: req.partition,
        offset,
    })
}

async fn handle_produce_batch(req: ProduceBatchRequest, registry: Arc<TopicRegistry>) -> Response {
    let tx = match registry
        .get_or_create_partition(&req.topic, req.partition)
        .await
    {
        Ok(tx) => tx,
        Err(e) => return ErrorResponse::from_error(req.correlation_id, ErrorCode::from(&e), e),
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    if let Err(e) = tx
        .send(BrokerCommand::ProduceBatch {
            payloads: req.payloads,
            reply: reply_tx,
        })
        .await
    {
        return ErrorResponse::from_error(
            req.correlation_id,
            ErrorCode::ServerError,
            format!("Writer task died: {}", e),
        );
    }

    let base_offset = match reply_rx.await {
        Ok(Ok(offset)) => offset,
        Ok(Err(e)) => return ErrorResponse::from_error(req.correlation_id, ErrorCode::from(&e), e),
        Err(e) => {
            return ErrorResponse::from_error(
                req.correlation_id,
                ErrorCode::ServerError,
                format!("Failed to receive reply: {}", e),
            )
        }
    };

    Response::ProduceBatch(ProduceBatchResponse {
        correlation_id: req.correlation_id,
        topic: req.topic,
        partition: req.partition,
        base_offset,
    })
}

async fn handle_fetch(req: FetchRequest, registry: Arc<TopicRegistry>) -> Response {
    let tx = match registry.get_partition(&req.topic, req.partition).await {
        Ok(tx) => tx,
        Err(e) => return ErrorResponse::from_error(req.correlation_id, ErrorCode::from(&e), e),
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    if let Err(e) = tx
        .send(BrokerCommand::Fetch {
            offset: req.offset,
            max_bytes: req.max_bytes,
            reply: reply_tx,
        })
        .await
    {
        return ErrorResponse::from_error(
            req.correlation_id,
            ErrorCode::ServerError,
            format!("Writer task died: {}", e),
        );
    }

    let records_res = match reply_rx.await {
        Ok(Ok(recs)) => recs,
        Ok(Err(e)) => return ErrorResponse::from_error(req.correlation_id, ErrorCode::from(&e), e),
        Err(e) => {
            return ErrorResponse::from_error(
                req.correlation_id,
                ErrorCode::ServerError,
                format!("Failed to receive reply: {}", e),
            )
        }
    };

    if records_res.is_empty() {
        let e = crate::error::RukaError::OffsetNotFound(req.offset);
        return ErrorResponse::from_error(req.correlation_id, ErrorCode::from(&e), e);
    }

    let mut payload = bytes::BytesMut::new();
    bytes::BufMut::put_u32(&mut payload, records_res.len() as u32);
    for record in records_res {
        record.encode(&mut payload);
    }

    Response::Fetch(FetchResponse {
        correlation_id: req.correlation_id,
        topic: req.topic,
        partition: req.partition,
        payload: payload.freeze(),
    })
}
