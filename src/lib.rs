//! Ruka: A high-performance append-only log and message broker.
//!
//! This crate implements a Kafka-inspired streaming engine with:
//! - Custom binary wire protocol
//! - Append-only segmented storage
//! - Efficient binary payload framing
//! - Per-partition concurrency

pub mod config;
pub mod error;
pub mod protocol;

pub mod broker;
pub mod network;
pub mod storage;
