//! Zero-copy and memory-mapped file transfer utilities.
//!
//! Provides optimized file reading capabilities to serve data to clients
//! with minimal CPU and memory overhead.

use std::fs::File;
use std::io;

use bytes::Bytes;
use memmap2::MmapOptions;

/// Reads a segment of a file using `memmap2`.
///
/// This provides a zero-copy read from the OS page cache into the process memory space.
/// We currently copy it into a `Bytes` struct for seamless integration with Tokio's `Framed`
/// codec, representing a 1-copy transfer which is highly performant and stable across all platforms.
///
/// True zero-copy (using `sendfile` on Linux) would require bypassing the `Framed` encoder
/// to write headers and payload separately directly to the `TcpStream` file descriptor.
pub fn mmap_read_to_bytes(file: &File, offset: u64, len: usize) -> io::Result<Bytes> {
    if len == 0 {
        return Ok(Bytes::new());
    }

    // Safety: we assume the underlying log files are strictly append-only and not
    // truncated or modified concurrently in a way that violates mmap safety.
    let mmap = unsafe { MmapOptions::new().offset(offset).len(len).map(file)? };

    Ok(Bytes::copy_from_slice(&mmap))
}
