//! Partition manager and segment rotation.
//!
//! A partition is an ordered sequence of segments. All writes go to the
//! active (newest) segment. When the active segment reaches the configured
//! size limit, a new segment is created and becomes the active segment.

use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::error::Result;
use crate::storage::segment::{Record, Segment};

/// Manages the append-only sequence of segment files for a single partition.
pub struct Partition {
    pub topic: String,
    pub id: u32,
    base_dir: PathBuf,
    segments: Vec<Segment>,
    max_segment_bytes: u64,
    sync_level: crate::config::SyncLevel,
}

impl Partition {
    /// Open an existing partition or create it if it doesn't exist.
    pub fn open_or_create(
        topic: String,
        id: u32,
        base_dir: impl AsRef<Path>,
        max_segment_bytes: u64,
        sync_level: crate::config::SyncLevel,
    ) -> Result<Self> {
        let partition_dir = base_dir.as_ref().join(&topic).join(id.to_string());
        fs::create_dir_all(&partition_dir)?;

        let mut segments = Vec::new();

        // Scan directory for existing segments
        for entry in fs::read_dir(&partition_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(ext) = path.extension() {
                if ext == "log" {
                    if let Some(stem) = path.file_stem() {
                        if let Ok(base_offset) = stem.to_string_lossy().parse::<u64>() {
                            let segment = Segment::load(
                                &partition_dir,
                                base_offset,
                                max_segment_bytes,
                                sync_level,
                            )?;
                            segments.push(segment);
                        }
                    }
                }
            }
        }

        // Sort segments by base_offset ascending
        segments.sort_by_key(|s| s.base_offset);

        // If no segments exist, create the first one at offset 0
        if segments.is_empty() {
            let segment = Segment::create(&partition_dir, 0, max_segment_bytes, sync_level)?;
            segments.push(segment);
        }

        Ok(Self {
            topic,
            id,
            base_dir: partition_dir,
            segments,
            max_segment_bytes,
            sync_level,
        })
    }

    /// Append a new record (key, value) and return its assigned offset.
    ///
    /// Automatically rotates to a new segment if the active segment is full.
    #[tracing::instrument(skip(self, key, value), fields(topic = %self.topic, partition = self.id))]
    pub fn append(&mut self, key: Option<Bytes>, value: Option<Bytes>) -> Result<u64> {
        let active_idx = self.segments.len() - 1;

        match self.segments[active_idx].append(key.clone(), value.clone()) {
            Ok(offset) => Ok(offset),
            Err(crate::error::RukaError::SegmentFull { .. }) => {
                let next_offset = self.segments[active_idx].next_offset();
                self.segments[active_idx].flush()?;
                let new_segment = Segment::create(
                    &self.base_dir,
                    next_offset,
                    self.max_segment_bytes,
                    self.sync_level,
                )?;
                self.segments.push(new_segment);
                let active_idx = self.segments.len() - 1;
                self.segments[active_idx].append(key, value)
            }
            Err(e) => Err(e),
        }
    }

    /// Append a batch of payloads and return the base offset of the first record.
    #[tracing::instrument(skip(self, payloads), fields(topic = %self.topic, partition = self.id, batch_size = payloads.len()))]
    pub fn append_batch(&mut self, mut payloads: Vec<Bytes>) -> Result<u64> {
        let mut base_offset = None;

        while !payloads.is_empty() {
            let active_idx = self.segments.len() - 1;

            match self.segments[active_idx].append_batch(&payloads) {
                Ok((offset, count)) => {
                    if base_offset.is_none() {
                        base_offset = Some(offset);
                    }
                    payloads = payloads.split_off(count);

                    if !payloads.is_empty() {
                        let next_offset = self.segments[active_idx].next_offset();
                        self.segments[active_idx].flush()?;
                        let new_segment = Segment::create(
                            &self.base_dir,
                            next_offset,
                            self.max_segment_bytes,
                            self.sync_level,
                        )?;
                        self.segments.push(new_segment);
                    }
                }
                Err(crate::error::RukaError::SegmentFull { .. }) => {
                    let next_offset = self.segments[active_idx].next_offset();
                    self.segments[active_idx].flush()?;
                    let new_segment = Segment::create(
                        &self.base_dir,
                        next_offset,
                        self.max_segment_bytes,
                        self.sync_level,
                    )?;
                    self.segments.push(new_segment);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(base_offset.unwrap_or_else(|| self.next_offset()))
    }

    /// Read a record at the given exact offset.
    #[tracing::instrument(skip(self), fields(topic = %self.topic, partition = self.id))]
    pub fn read(&mut self, offset: u64) -> Result<Option<Record>> {
        // Find which segment should contain this offset
        // Binary search the segments array based on base_offset
        let segment_idx = match self
            .segments
            .binary_search_by_key(&offset, |s| s.base_offset)
        {
            Ok(idx) => idx, // Exact match on base_offset
            Err(idx) => {
                if idx == 0 {
                    // Offset is before our oldest segment
                    return Ok(None);
                }
                idx - 1 // The segment with base_offset <= offset
            }
        };

        self.segments[segment_idx].read_at(offset)
    }

    /// Read a batch of records starting from the given offset, up to max_bytes.
    #[tracing::instrument(skip(self), fields(topic = %self.topic, partition = self.id))]
    pub fn read_batch(&mut self, mut offset: u64, max_bytes: u32) -> Result<Vec<Record>> {
        let mut results = Vec::new();
        let mut bytes_budget = max_bytes;

        let mut segment_idx = match self
            .segments
            .binary_search_by_key(&offset, |s| s.base_offset)
        {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    return Ok(results);
                }
                idx - 1
            }
        };

        while segment_idx < self.segments.len() && bytes_budget > 0 {
            let segment = &mut self.segments[segment_idx];
            let records = segment.read_batch(offset, bytes_budget)?;

            if records.is_empty() {
                // Try next segment
                segment_idx += 1;
                if segment_idx < self.segments.len() {
                    offset = self.segments[segment_idx].base_offset;
                }
                continue;
            }

            for r in records {
                let rec_len = 28
                    + r.key.as_ref().map_or(0, |k| k.len()) as u32
                    + r.value.as_ref().map_or(0, |v| v.len()) as u32;
                if bytes_budget >= rec_len {
                    bytes_budget -= rec_len;
                    offset = r.offset + 1;
                    results.push(r);
                } else if results.is_empty() {
                    results.push(r);
                    return Ok(results);
                } else {
                    return Ok(results);
                }
            }

            // If we consumed all records from this segment but still have budget,
            // we should try the next segment in the next iteration.
            segment_idx += 1;
            if segment_idx < self.segments.len() {
                offset = self.segments[segment_idx].base_offset;
            }
        }

        Ok(results)
    }

    /// Flush all buffers to the OS.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(active) = self.segments.last_mut() {
            active.flush()?;
        }
        Ok(())
    }

    /// Sync data to persistent storage.
    pub fn sync(&mut self) -> Result<()> {
        if let Some(active) = self.segments.last_mut() {
            active.sync()?;
        }
        Ok(())
    }

    /// Get the next offset to be written in this partition.
    pub fn next_offset(&self) -> u64 {
        self.segments.last().map(|s| s.next_offset()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn partition_append_and_read() {
        let dir = TempDir::new().unwrap();
        let mut partition = Partition::open_or_create(
            "topicA".to_string(),
            0,
            dir.path(),
            1024 * 1024,
            crate::config::SyncLevel::LogAndIndex,
        )
        .unwrap();

        let val1 = Bytes::from_static(b"data1");
        let offset = partition.append(None, Some(val1.clone())).unwrap();
        assert_eq!(offset, 0);

        let rec = partition.read(0).unwrap().unwrap();
        assert_eq!(rec.value, Some(val1));
    }

    #[test]
    fn partition_segment_rotation() {
        let dir = TempDir::new().unwrap();

        // Very small max segment size (100 bytes) to force rotation
        let mut partition = Partition::open_or_create(
            "topicB".to_string(),
            0,
            dir.path(),
            100,
            crate::config::SyncLevel::LogAndIndex,
        )
        .unwrap();

        // Append multiple records
        let val = Bytes::from(vec![0u8; 40]);
        for _ in 0..10 {
            partition.append(None, Some(val.clone())).unwrap();
        }

        // We should have multiple segments now
        assert!(partition.segments.len() > 1, "Should have rotated segments");

        // We should be able to read all of them back
        for i in 0..10 {
            let rec = partition.read(i).unwrap().unwrap();
            assert_eq!(rec.offset, i);
        }
    }

    #[test]
    fn partition_reload() {
        let dir = TempDir::new().unwrap();

        {
            let mut partition = Partition::open_or_create(
                "topicC".to_string(),
                0,
                dir.path(),
                1024,
                crate::config::SyncLevel::LogAndIndex,
            )
            .unwrap();
            partition
                .append(None, Some(Bytes::from_static(b"test")))
                .unwrap();
            partition.flush().unwrap();
        }

        // Reload
        let mut partition2 = Partition::open_or_create(
            "topicC".to_string(),
            0,
            dir.path(),
            1024,
            crate::config::SyncLevel::LogAndIndex,
        )
        .unwrap();
        let rec = partition2.read(0).unwrap().unwrap();
        assert_eq!(rec.value, Some(Bytes::from_static(b"test")));
    }
}
