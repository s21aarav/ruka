//! Companion index file for segment-level offset lookups.
//!
//! Each `.index` file contains a sorted array of fixed-size entries:
//! ```text
//! [MessageOffset: 8B (u64 big-endian)][PhysicalPosition: 8B (u64 big-endian)]
//! ```
//!
//! Every entry is exactly 16 bytes, enabling O(log N) binary search
//! for any discrete offset.
//!
//! The index entries are also held in memory for fast lookups, while
//! the file is used for persistence and crash recovery.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Result, RukaError};

/// Size of a single index entry in bytes: offset(8) + position(8) = 16.
pub const INDEX_ENTRY_SIZE: usize = 16;

/// A single index entry mapping a logical message offset to its
/// physical byte position within the corresponding `.log` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    /// The logical message offset (monotonically increasing).
    pub offset: u64,
    /// The byte offset into the `.log` file where this record starts.
    pub position: u64,
}

impl IndexEntry {
    /// Serialize this entry to a 16-byte big-endian array.
    pub fn encode(&self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&self.offset.to_be_bytes());
        buf[8..16].copy_from_slice(&self.position.to_be_bytes());
        buf
    }

    /// Deserialize an entry from a 16-byte big-endian array.
    pub fn decode(buf: &[u8; INDEX_ENTRY_SIZE]) -> Self {
        let offset = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let position = u64::from_be_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        Self { offset, position }
    }
}

/// An index file managing `[Offset:8B][Position:8B]` entries for a single segment.
///
/// Entries are maintained both on disk (for durability) and in memory
/// (for fast binary-search lookups without re-reading the file).
pub struct Index {
    /// Path to the `.index` file on disk.
    path: PathBuf,
    /// File handle used for appending new entries.
    file: File,
    /// In-memory mirror of all index entries, sorted by offset.
    entries: Vec<IndexEntry>,
}

impl Index {
    /// Create a new, empty index file at `path`.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)?;

        Ok(Self {
            path,
            file,
            entries: Vec::new(),
        })
    }

    /// Load an existing index file from disk, reading all entries into memory.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        let file_len = file.metadata()?.len() as usize;

        // Validate file length is a multiple of entry size
        if !file_len.is_multiple_of(INDEX_ENTRY_SIZE) {
            return Err(RukaError::CorruptedIndex(file_len as u64));
        }

        let entry_count = file_len / INDEX_ENTRY_SIZE;
        let mut entries = Vec::with_capacity(entry_count);

        file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; INDEX_ENTRY_SIZE];
        for _ in 0..entry_count {
            file.read_exact(&mut buf)?;
            entries.push(IndexEntry::decode(&buf));
        }

        // Position at end for future appends
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path,
            file,
            entries,
        })
    }

    /// Append a new entry mapping `offset` → `position`.
    ///
    /// Writes the 16-byte entry to disk and adds it to the in-memory vector.
    /// The caller is responsible for calling [`flush`] to ensure durability.
    pub fn append(&mut self, offset: u64, position: u64) -> Result<()> {
        let entry = IndexEntry { offset, position };
        self.file.write_all(&entry.encode())?;
        self.entries.push(entry);
        Ok(())
    }

    /// Look up the physical position for an exact message offset.
    ///
    /// Uses binary search over the in-memory entry vector.
    /// Returns `None` if the exact offset is not indexed.
    pub fn lookup(&self, target_offset: u64) -> Option<u64> {
        match self
            .entries
            .binary_search_by_key(&target_offset, |e| e.offset)
        {
            Ok(idx) => Some(self.entries[idx].position),
            Err(_) => None,
        }
    }

    /// Find the physical position for the largest indexed offset ≤ `target_offset`.
    ///
    /// Useful for sparse indexes or range scans.
    /// Returns `None` if no entry has an offset ≤ `target_offset`.
    #[allow(dead_code)]
    pub fn lookup_floor(&self, target_offset: u64) -> Option<IndexEntry> {
        match self
            .entries
            .binary_search_by_key(&target_offset, |e| e.offset)
        {
            Ok(idx) => Some(self.entries[idx]),
            Err(0) => None,
            Err(idx) => Some(self.entries[idx - 1]),
        }
    }

    /// Flush the underlying file handle to the OS.
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    /// Sync the index file data to persistent storage.
    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Number of entries currently held.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Read-only access to the in-memory entries.
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Path to the `.index` file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn entry_round_trip() {
        let entry = IndexEntry {
            offset: 42,
            position: 1024,
        };
        let encoded = entry.encode();
        assert_eq!(encoded.len(), INDEX_ENTRY_SIZE);

        let decoded = IndexEntry::decode(&encoded);
        assert_eq!(entry, decoded);
    }

    #[test]
    fn entry_byte_layout_is_16_bytes_big_endian() {
        let entry = IndexEntry {
            offset: 0x0102030405060708,
            position: 0x090A0B0C0D0E0F10,
        };
        let buf = entry.encode();
        assert_eq!(
            buf,
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // offset BE
                0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // position BE
            ]
        );
    }

    #[test]
    fn create_append_lookup() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.index");

        let mut index = Index::create(&path).unwrap();
        index.append(0, 0).unwrap();
        index.append(1, 100).unwrap();
        index.append(2, 250).unwrap();

        assert_eq!(index.lookup(0), Some(0));
        assert_eq!(index.lookup(1), Some(100));
        assert_eq!(index.lookup(2), Some(250));
        assert_eq!(index.lookup(3), None);
        assert_eq!(index.entry_count(), 3);
    }

    #[test]
    fn lookup_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.index");

        let index = Index::create(&path).unwrap();
        assert_eq!(index.lookup(0), None);
        assert_eq!(index.lookup(999), None);
    }

    #[test]
    fn lookup_floor() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("floor.index");

        let mut index = Index::create(&path).unwrap();
        index.append(0, 0).unwrap();
        index.append(10, 500).unwrap();
        index.append(20, 1200).unwrap();

        // Exact match
        let e = index.lookup_floor(10).unwrap();
        assert_eq!(e.offset, 10);
        assert_eq!(e.position, 500);

        // Floor: 7 → entry for offset 0
        let e = index.lookup_floor(7).unwrap();
        assert_eq!(e.offset, 0);

        // Floor: 15 → entry for offset 10
        let e = index.lookup_floor(15).unwrap();
        assert_eq!(e.offset, 10);

        // No floor for offsets below the first entry
        // (we have offset 0, so only truly negative would fail — but offsets are u64)
        assert!(index.lookup_floor(0).is_some());
    }

    #[test]
    fn save_and_reload() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("persist.index");

        // Write entries
        {
            let mut index = Index::create(&path).unwrap();
            for i in 0..100 {
                index.append(i, i * 64).unwrap();
            }
            index.flush().unwrap();
        }

        // Reload and verify
        let index = Index::load(&path).unwrap();
        assert_eq!(index.entry_count(), 100);

        for i in 0..100 {
            assert_eq!(index.lookup(i), Some(i * 64));
        }
    }

    #[test]
    fn file_size_is_exact() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("size.index");

        let mut index = Index::create(&path).unwrap();
        for i in 0..50 {
            index.append(i, i * 100).unwrap();
        }
        index.flush().unwrap();

        let file_size = std::fs::metadata(&path).unwrap().len();
        assert_eq!(file_size, 50 * INDEX_ENTRY_SIZE as u64);
    }
}
