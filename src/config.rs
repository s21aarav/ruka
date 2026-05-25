use clap::Parser;
use std::path::PathBuf;

/// Ruka broker configuration.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "ruka",
    about = "A high-performance append-only log and message broker"
)]
pub struct BrokerConfig {
    /// Port to listen on.
    #[arg(short, long, default_value_t = 9092)]
    pub port: u16,

    /// Bind address.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Root directory for segment data storage.
    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Maximum segment file size in bytes before rotation.
    /// Defaults to 10 MB.
    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    pub segment_max_bytes: u64,

    /// Number of default partitions for new topics.
    #[arg(long, default_value_t = 1)]
    pub default_partitions: u32,

    /// Sync level after each write.
    #[arg(long, default_value = "log-and-index", value_enum)]
    pub sync_level: SyncLevel,

    /// Channel capacity for partition actor mailboxes (backpressure limit).
    #[arg(long, default_value_t = 1024)]
    pub channel_capacity: usize,
}

/// Level of fsync guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum SyncLevel {
    /// Never sync automatically
    None,
    /// Sync the `.log` file only
    Log,
    /// Sync both `.log` and `.index`
    #[default]
    LogAndIndex,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            port: 9092,
            host: "127.0.0.1".to_string(),
            data_dir: PathBuf::from("./data"),
            segment_max_bytes: 10 * 1024 * 1024,
            default_partitions: 1,
            sync_level: SyncLevel::LogAndIndex,
            channel_capacity: 1024,
        }
    }
}
