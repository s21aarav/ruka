//! Segment file management.
//!
//! A segment consists of a `.log` file containing the actual message data
//! and a companion `.index` file mapping offsets to physical positions.
//!
//! Each record in the `.log` file is framed as:
//! ```text
//! [RecordLength: 4B][Offset: 8B][TimestampMs: 8B][KeyLength: 4B][Key: VarB][ValueLength: 4B][Value: VarB]
//! ```
//!
//! - `RecordLength` includes itself (4 bytes) plus all subsequent fields.
//! - A `KeyLength` or `ValueLength` of `u32::MAX` (0xFFFFFFFF) means `None`.
//! - A length of `0` means empty bytes (`Some(Bytes::new())`).

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Result, RukaError};
use crate::storage::index::Index;

/// Represents a discrete record as stored on disk and read from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

impl Record {
    /// Encode the record into a buffer.
    pub fn encode(&self, dst: &mut BytesMut) {
        let key_len = self.key.as_ref().map_or(0, |k| k.len());
        let val_len = self.value.as_ref().map_or(0, |v| v.len());
        // 4 (prefix) + 8 (offset) + 8 (timestamp) + 4 (keylen) + key_len + 4 (vallen) + val_len
        let record_len = 4 + 8 + 8 + 4 + key_len + 4 + val_len;

        dst.put_u32(record_len as u32);
        dst.put_u64(self.offset);
        dst.put_u64(self.timestamp_ms);

        if let Some(key) = &self.key {
            dst.put_u32(key.len() as u32);
            dst.put_slice(key);
        } else {
            dst.put_u32(u32::MAX);
        }

        if let Some(value) = &self.value {
            dst.put_u32(value.len() as u32);
            dst.put_slice(value);
        } else {
            dst.put_u32(u32::MAX);
        }
    }

    /// Decode a record from a buffer.
    /// Returns Ok(None) if not enough bytes.
    pub fn decode(src: &mut BytesMut) -> Result<Option<Record>> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut peek = src.clone();
        let record_len = peek.get_u32() as usize;

        if record_len < 28 {
            return Err(RukaError::InvalidFrame(
                "Record length too small".to_string(),
            ));
        }

        if src.len() < record_len {
            return Ok(None);
        }

        // We have the full record, consume prefix
        src.advance(4);

        let offset = src.get_u64();
        let timestamp_ms = src.get_u64();

        let key_len = src.get_u32();
        let key = if key_len == u32::MAX {
            None
        } else {
            Some(src.split_to(key_len as usize).freeze())
        };

        let val_len = src.get_u32();
        let value = if val_len == u32::MAX {
            None
        } else {
            Some(src.split_to(val_len as usize).freeze())
        };

        Ok(Some(Record {
            offset,
            timestamp_ms,
            key,
            value,
        }))
    }
}

/// A segment manages a `.log` file and its `.index`.
pub struct Segment {
    pub base_offset: u64,
    log_file: File,
    index: Index,
    current_size: u64,
    next_offset: u64,
    max_bytes: u64,
    sync_level: crate::config::SyncLevel,
}

impl Segment {
    /// Create a new segment at `base_dir` for `base_offset`.
    pub fn create(
        base_dir: impl AsRef<Path>,
        base_offset: u64,
        max_bytes: u64,
        sync_level: crate::config::SyncLevel,
    ) -> Result<Self> {
        let dir = base_dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let prefix = format!("{:020}", base_offset);
        let log_path = dir.join(format!("{}.log", prefix));
        let index_path = dir.join(format!("{}.index", prefix));

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&log_path)?;

        let index = Index::create(&index_path)?;

        Ok(Self {
            base_offset,
            log_file,
            index,
            current_size: 0,
            next_offset: base_offset,
            max_bytes,
            sync_level,
        })
    }

    /// Load an existing segment.
    pub fn load(
        base_dir: impl AsRef<Path>,
        base_offset: u64,
        max_bytes: u64,
        sync_level: crate::config::SyncLevel,
    ) -> Result<Self> {
        let dir = base_dir.as_ref();
        let prefix = format!("{:020}", base_offset);
        let log_path = dir.join(format!("{}.log", prefix));
        let index_path = dir.join(format!("{}.index", prefix));

        let mut log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;

        let mut index = match Index::load(&index_path) {
            Ok(idx) => idx,
            Err(_) => {
                // Rebuild if corrupted or missing
                Index::create(&index_path)?
            }
        };

        let file_len = log_file.metadata()?.len();

        log_file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        log_file.read_to_end(&mut buf)?;
        let mut src = BytesMut::from(&buf[..]);

        let mut current_offset = base_offset;
        let mut valid_bytes = 0;
        let rebuild_index = index.entry_count() == 0 && file_len > 0;

        loop {
            let start_len = src.len();
            match Record::decode(&mut src) {
                Ok(Some(record)) => {
                    let record_len = start_len - src.len();

                    if rebuild_index {
                        index.append(record.offset, valid_bytes as u64)?;
                    }

                    valid_bytes += record_len;
                    current_offset = record.offset + 1;
                }
                Ok(None) | Err(_) => {
                    // Partial or corrupted frame
                    break;
                }
            }
        }

        if valid_bytes < file_len as usize {
            log_file.set_len(valid_bytes as u64)?;
        }

        if rebuild_index {
            index.flush()?;
        }

        log_file.seek(SeekFrom::End(0))?;

        Ok(Self {
            base_offset,
            log_file,
            index,
            current_size: valid_bytes as u64,
            next_offset: current_offset,
            max_bytes,
            sync_level,
        })
    }

    /// Append a new record (key, value) and return its assigned offset.
    #[tracing::instrument(skip(self, key, value), fields(base_offset = self.base_offset))]
    pub fn append(&mut self, key: Option<Bytes>, value: Option<Bytes>) -> Result<u64> {
        let key_len = key.as_ref().map_or(0, |k| k.len());
        let val_len = value.as_ref().map_or(0, |v| v.len());
        let record_len = 4 + 8 + 8 + 4 + key_len + 4 + val_len;

        if record_len as u64 > self.max_bytes {
            return Err(RukaError::MessageTooLarge);
        }

        if self.current_size + record_len as u64 > self.max_bytes && self.current_size > 0 {
            return Err(RukaError::SegmentFull {
                current: self.current_size,
                max: self.max_bytes,
            });
        }

        let offset = self.next_offset;
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let record = Record {
            offset,
            timestamp_ms,
            key,
            value,
        };

        let mut buf = BytesMut::new();
        record.encode(&mut buf);
        let data = buf.freeze();

        let physical_position = self.current_size;

        self.log_file.write_all(&data)?;

        if matches!(
            self.sync_level,
            crate::config::SyncLevel::Log | crate::config::SyncLevel::LogAndIndex
        ) {
            self.log_file.sync_data()?;
        }

        // Add to index
        self.index.append(offset, physical_position)?;

        if matches!(self.sync_level, crate::config::SyncLevel::LogAndIndex) {
            self.index.sync()?;
        }

        self.current_size += data.len() as u64;
        self.next_offset += 1;

        Ok(offset)
    }

    /// Append a batch of payloads and return the base offset of the first record.
    #[tracing::instrument(skip(self, payloads), fields(base_offset = self.base_offset))]
    pub fn append_batch(&mut self, payloads: &[Bytes]) -> Result<(u64, usize)> {
        if payloads.is_empty() {
            return Ok((self.next_offset, 0));
        }

        let base_offset = self.next_offset;
        let mut buf = BytesMut::new();
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut physical_position = self.current_size;
        let mut current_offset = base_offset;
        let mut index_entries = Vec::new();
        let mut appended_count = 0;

        for payload in payloads {
            let record_len = 28 + payload.len() as u64; // 4+8+8+4+0+4+len
            if record_len > self.max_bytes {
                return Err(RukaError::MessageTooLarge);
            }

            if physical_position + record_len > self.max_bytes {
                if appended_count > 0 {
                    break;
                } else {
                    return Err(RukaError::SegmentFull {
                        current: self.current_size,
                        max: self.max_bytes,
                    });
                }
            }

            let record = Record {
                offset: current_offset,
                timestamp_ms,
                key: None,
                value: Some(payload.clone()),
            };

            let start_len = buf.len();
            record.encode(&mut buf);
            let bytes_added = (buf.len() - start_len) as u64;

            index_entries.push((current_offset, physical_position));

            physical_position += bytes_added;
            current_offset += 1;
            appended_count += 1;
        }

        let data = buf.freeze();
        if !data.is_empty() {
            self.log_file.write_all(&data)?;

            if matches!(
                self.sync_level,
                crate::config::SyncLevel::Log | crate::config::SyncLevel::LogAndIndex
            ) {
                self.log_file.sync_data()?;
            }

            for (off, pos) in index_entries {
                self.index.append(off, pos)?;
            }

            if matches!(self.sync_level, crate::config::SyncLevel::LogAndIndex) {
                self.index.sync()?;
            }

            self.current_size += data.len() as u64;
            self.next_offset = current_offset;
        }

        Ok((base_offset, appended_count))
    }

    /// Read a record at exactly the given offset.
    #[tracing::instrument(skip(self), fields(base_offset = self.base_offset))]
    pub fn read_at(&mut self, offset: u64) -> Result<Option<Record>> {
        if offset < self.base_offset || offset >= self.next_offset {
            return Ok(None);
        }

        let position = match self.index.lookup(offset) {
            Some(p) => p,
            None => {
                // If sparse index, we'd lookup_floor and scan. For now, we index every message.
                // Assuming we index every message, if it's not in the index, we don't have it.
                return Ok(None);
            }
        };

        self.log_file.seek(SeekFrom::Start(position))?;

        // Read enough bytes. In a real impl we might read length prefix first.
        // We know max frame size is bounded. Let's read a chunk.
        // For simplicity, we can read 4KB and decode, if not enough, read more.
        let mut buf = vec![0u8; 4096];
        let bytes_read = self.log_file.read(&mut buf)?;

        let mut src = BytesMut::from(&buf[..bytes_read]);
        if let Some(record) = Record::decode(&mut src)? {
            if record.offset == offset {
                return Ok(Some(record));
            }
        }

        // If 4KB wasn't enough, read the exact record length
        self.log_file.seek(SeekFrom::Start(position))?;
        let mut len_buf = [0u8; 4];
        self.log_file.read_exact(&mut len_buf)?;
        let record_len = u32::from_be_bytes(len_buf) as usize;

        let mut full_buf = vec![0u8; record_len];
        full_buf[0..4].copy_from_slice(&len_buf);
        self.log_file.read_exact(&mut full_buf[4..])?;

        let mut src = BytesMut::from(&full_buf[..]);
        Record::decode(&mut src)
    }

    /// Read a batch of records starting from offset, up to max_bytes.
    #[tracing::instrument(skip(self), fields(base_offset = self.base_offset))]
    pub fn read_batch(&mut self, offset: u64, max_bytes: u32) -> Result<Vec<Record>> {
        if offset < self.base_offset || offset >= self.next_offset {
            return Ok(Vec::new());
        }

        let position = match self.index.lookup(offset) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        self.log_file.seek(SeekFrom::Start(position))?;

        let mut max_read_size = max_bytes as usize;
        if max_read_size < 1024 * 1024 {
            max_read_size = 1024 * 1024; // Ensure we read a decent chunk
        }

        let mut buf = vec![0u8; max_read_size];
        let bytes_read = self.log_file.read(&mut buf)?;

        let mut src = BytesMut::from(&buf[..bytes_read]);
        let mut records = Vec::new();
        let mut total_bytes = 0;

        while src.len() >= 4 {
            let mut peek = src.clone();
            let record_len = peek.get_u32() as usize;

            if src.len() < record_len {
                break;
            }

            if total_bytes + record_len > max_bytes as usize && !records.is_empty() {
                break;
            }

            if let Ok(Some(record)) = Record::decode(&mut src) {
                if record.offset >= offset {
                    total_bytes += record_len;
                    records.push(record);
                }
            } else {
                break;
            }
        }

        if records.is_empty() && bytes_read > 0 {
            // Read exact first record if it exceeds buffer
            self.log_file.seek(SeekFrom::Start(position))?;
            let mut len_buf = [0u8; 4];
            if self.log_file.read_exact(&mut len_buf).is_ok() {
                let record_len = u32::from_be_bytes(len_buf) as usize;
                let mut full_buf = vec![0u8; record_len];
                full_buf[0..4].copy_from_slice(&len_buf);
                if self.log_file.read_exact(&mut full_buf[4..]).is_ok() {
                    let mut src = BytesMut::from(&full_buf[..]);
                    if let Ok(Some(record)) = Record::decode(&mut src) {
                        if record.offset >= offset {
                            records.push(record);
                        }
                    }
                }
            }
        }

        Ok(records)
    }

    /// Get the next offset that will be written.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Check if segment has reached max size.
    pub fn is_full(&self) -> bool {
        self.current_size >= self.max_bytes
    }

    /// Flush buffers to OS.
    pub fn flush(&mut self) -> Result<()> {
        self.log_file.flush()?;
        self.index.flush()?;
        Ok(())
    }

    /// Sync data to persistent storage.
    pub fn sync(&mut self) -> Result<()> {
        self.log_file.sync_data()?;
        self.index.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn segment_create_append_read() {
        let dir = TempDir::new().unwrap();
        let mut segment = Segment::create(
            dir.path(),
            100,
            1024 * 1024,
            crate::config::SyncLevel::LogAndIndex,
        )
        .unwrap();

        let val1 = Bytes::from_static(b"value1");
        let val2 = Bytes::from_static(b"value2");

        let offset1 = segment.append(None, Some(val1.clone())).unwrap();
        assert_eq!(offset1, 100);

        let offset2 = segment
            .append(Some(Bytes::from_static(b"key2")), Some(val2.clone()))
            .unwrap();
        assert_eq!(offset2, 101);

        segment.flush().unwrap();

        let rec1 = segment.read_at(100).unwrap().unwrap();
        assert_eq!(rec1.offset, 100);
        assert_eq!(rec1.value, Some(val1));

        let rec2 = segment.read_at(101).unwrap().unwrap();
        assert_eq!(rec2.offset, 101);
        assert_eq!(rec2.key, Some(Bytes::from_static(b"key2")));
        assert_eq!(rec2.value, Some(val2));

        assert!(segment.read_at(102).unwrap().is_none());
    }

    #[test]
    fn segment_reload() {
        let dir = TempDir::new().unwrap();

        {
            let mut segment =
                Segment::create(dir.path(), 0, 1024, crate::config::SyncLevel::LogAndIndex)
                    .unwrap();
            segment
                .append(None, Some(Bytes::from_static(b"hello")))
                .unwrap();
            segment
                .append(None, Some(Bytes::from_static(b"world")))
                .unwrap();
            segment.flush().unwrap();
        }

        let mut segment2 =
            Segment::load(dir.path(), 0, 1024, crate::config::SyncLevel::LogAndIndex).unwrap();
        assert_eq!(segment2.next_offset(), 2);

        let rec = segment2.read_at(1).unwrap().unwrap();
        assert_eq!(rec.value, Some(Bytes::from_static(b"world")));
    }
}
