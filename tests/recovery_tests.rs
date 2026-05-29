//! Recovery and robustness integration tests for ruka storage.

use bytes::Bytes;
use std::io::Write;
use tempfile::TempDir;

use ruka::config::SyncLevel;
use ruka::storage::partition::Partition;
use ruka::storage::segment::Segment;

// ---------------------------------------------------------------------------
// 1. Empty payload (Some(empty bytes)) round-trips as Some, not None
// ---------------------------------------------------------------------------
#[test]
fn empty_payload_is_not_none() {
    let dir = TempDir::new().unwrap();
    let mut seg = Segment::create(dir.path(), 0, 1024, SyncLevel::None).unwrap();

    let offset = seg.append(None, Some(Bytes::new())).unwrap();
    assert_eq!(offset, 0);

    seg.flush().unwrap();

    let rec = seg.read_at(0).unwrap().expect("record must exist");
    assert_eq!(rec.offset, 0);
    // The value must be Some(empty), NOT None.
    assert_eq!(rec.value, Some(Bytes::new()));
    assert!(rec.key.is_none());
}

// ---------------------------------------------------------------------------
// 2. None payload round-trips as None
// ---------------------------------------------------------------------------
#[test]
fn none_payload_round_trips() {
    let dir = TempDir::new().unwrap();
    let mut seg = Segment::create(dir.path(), 0, 1024, SyncLevel::None).unwrap();

    let offset = seg.append(None, None).unwrap();
    assert_eq!(offset, 0);

    seg.flush().unwrap();

    let rec = seg.read_at(0).unwrap().expect("record must exist");
    assert_eq!(rec.offset, 0);
    assert!(
        rec.value.is_none(),
        "value should be None, got {:?}",
        rec.value
    );
    assert!(rec.key.is_none());
}

// ---------------------------------------------------------------------------
// 3. Batch splits across segments when max_segment_bytes is small
// ---------------------------------------------------------------------------
#[test]
fn batch_splits_across_segments() {
    let dir = TempDir::new().unwrap();
    let max_segment_bytes: u64 = 200;

    let mut partition = Partition::open_or_create(
        "test_topic".to_string(),
        0,
        dir.path(),
        max_segment_bytes,
        SyncLevel::None,
    )
    .unwrap();

    // Each payload is ~30 bytes, record overhead is 28 bytes -> ~58 bytes per record.
    // 200 / 58 ~= 3 records per segment, so 10 records should span multiple segments.
    let payloads: Vec<Bytes> = (0..10)
        .map(|i| Bytes::from(format!("payload-{:020}", i)))
        .collect();

    let base_offset = partition.append_batch(payloads.clone()).unwrap();
    assert_eq!(base_offset, 0);

    partition.flush().unwrap();

    // Count .log files in the partition directory to verify multiple segments.
    let partition_dir = dir.path().join("test_topic").join("0");
    let log_count = std::fs::read_dir(&partition_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
        .count();
    assert!(
        log_count > 1,
        "Expected multiple segments, but found {} .log files",
        log_count
    );

    // Read all records back and verify they match.
    for (i, payload) in payloads.iter().enumerate() {
        let rec = partition
            .read(i as u64)
            .unwrap()
            .unwrap_or_else(|| panic!("record at offset {} must exist", i));
        assert_eq!(rec.offset, i as u64);
        assert_eq!(rec.value.as_ref(), Some(payload));
    }
}

// ---------------------------------------------------------------------------
// 4. Missing index file is rebuilt from the log
// ---------------------------------------------------------------------------
#[test]
fn missing_index_rebuilds_from_log() {
    let dir = TempDir::new().unwrap();

    // Create and populate segment, then flush.
    {
        let mut seg = Segment::create(dir.path(), 0, 1024 * 1024, SyncLevel::None).unwrap();
        for i in 0..5u64 {
            seg.append(None, Some(Bytes::from(format!("val-{}", i))))
                .unwrap();
        }
        seg.flush().unwrap();
    }

    // Delete the .index file.
    let index_path = dir.path().join("00000000000000000000.index");
    assert!(
        index_path.exists(),
        "index file should exist before deletion"
    );
    std::fs::remove_file(&index_path).unwrap();
    assert!(!index_path.exists());

    // Reload — the index should be rebuilt from the log.
    let mut seg = Segment::load(dir.path(), 0, 1024 * 1024, SyncLevel::None).unwrap();
    assert_eq!(seg.next_offset(), 5);

    for i in 0..5u64 {
        let rec = seg.read_at(i).unwrap().unwrap_or_else(|| {
            panic!(
                "record at offset {} must be readable after index rebuild",
                i
            )
        });
        assert_eq!(rec.offset, i);
        assert_eq!(rec.value, Some(Bytes::from(format!("val-{}", i))));
    }
}

// ---------------------------------------------------------------------------
// 5. Corrupt index (garbage bytes) is rebuilt / recovered
// ---------------------------------------------------------------------------
#[test]
fn corrupt_index_rebuilds_or_recovers() {
    let dir = TempDir::new().unwrap();

    // Create and populate segment.
    {
        let mut seg = Segment::create(dir.path(), 0, 1024 * 1024, SyncLevel::None).unwrap();
        for i in 0..5u64 {
            seg.append(None, Some(Bytes::from(format!("val-{}", i))))
                .unwrap();
        }
        seg.flush().unwrap();
    }

    // Overwrite the .index file with 7 garbage bytes (not a multiple of 16).
    let index_path = dir.path().join("00000000000000000000.index");
    {
        let mut f = std::fs::File::create(&index_path).unwrap();
        f.write_all(&[0xFF; 7]).unwrap();
        f.flush().unwrap();
    }

    // Reload — should rebuild the index from the log.
    let mut seg = Segment::load(dir.path(), 0, 1024 * 1024, SyncLevel::None).unwrap();
    assert_eq!(seg.next_offset(), 5);

    for i in 0..5u64 {
        let rec = seg.read_at(i).unwrap().unwrap_or_else(|| {
            panic!(
                "record at offset {} must be readable after corrupt-index recovery",
                i
            )
        });
        assert_eq!(rec.offset, i);
        assert_eq!(rec.value, Some(Bytes::from(format!("val-{}", i))));
    }
}

// ---------------------------------------------------------------------------
// 6. Partial trailing record in the .log is truncated on reload
// ---------------------------------------------------------------------------
#[test]
fn partial_trailing_record_is_truncated() {
    let dir = TempDir::new().unwrap();

    // Create and populate segment with 3 records.
    {
        let mut seg = Segment::create(dir.path(), 0, 1024 * 1024, SyncLevel::None).unwrap();
        for i in 0..3u64 {
            seg.append(None, Some(Bytes::from(format!("rec-{}", i))))
                .unwrap();
        }
        seg.flush().unwrap();
    }

    // Append raw garbage to the end of the .log file:
    // This looks like a length-prefix claiming 80 bytes, but no data follows.
    let log_path = dir.path().join("00000000000000000000.log");
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        f.write_all(&[0x00, 0x00, 0x00, 0x50]).unwrap(); // 0x50 = 80 bytes
        f.flush().unwrap();
    }

    // Reload — the partial trailing record should be truncated.
    let mut seg = Segment::load(dir.path(), 0, 1024 * 1024, SyncLevel::None).unwrap();
    assert_eq!(
        seg.next_offset(),
        3,
        "next_offset should be 3 (partial record truncated)"
    );

    // All 3 original records should still be readable.
    for i in 0..3u64 {
        let rec = seg
            .read_at(i)
            .unwrap()
            .unwrap_or_else(|| panic!("record at offset {} must survive truncation", i));
        assert_eq!(rec.offset, i);
        assert_eq!(rec.value, Some(Bytes::from(format!("rec-{}", i))));
    }

    // Offset 3 should not exist.
    assert!(seg.read_at(3).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// 7. A single message larger than the segment max is rejected
// ---------------------------------------------------------------------------
#[test]
fn single_message_larger_than_segment_is_rejected() {
    let dir = TempDir::new().unwrap();
    let mut seg = Segment::create(dir.path(), 0, 100, SyncLevel::None).unwrap();

    // Value of 200 bytes → total record > 100 bytes. Should be rejected.
    let big_value = Bytes::from(vec![0xAB; 200]);
    let result = seg.append(None, Some(big_value));

    assert!(
        result.is_err(),
        "appending a message larger than max_bytes should fail"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{}", err);
    assert!(
        err_msg.contains("too large") || err_msg.contains("full"),
        "error should indicate MessageTooLarge or SegmentFull, got: {}",
        err_msg
    );
}
