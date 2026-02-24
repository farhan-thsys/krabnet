//! Write-ahead log for crash recovery.
//!
//! Binary append-only log recording every [`Event`] with its assigned epoch.
//! Format per entry: `[u32 length][u64 epoch][serialized Event bytes]`.
//! Supports configurable fsync interval (default: every 1000 events)
//! and explicit flush for immediate durability.
//!
//! # Architecture
//!
//! [`WalWriter`] appends events in binary format to a file. After every
//! `fsync_interval` events, it flushes and fsyncs to disk. An explicit
//! [`flush()`](WalWriter::flush) method allows callers to force durability
//! at any point (e.g., graceful shutdown).
//!
//! [`WalReader`] replays all entries from a WAL file, returning a vector of
//! `(Epoch, Event)` pairs. It stops at the first incomplete entry, supporting
//! crash recovery up to the last complete write.
//!
//! # Example
//!
//! ```no_run
//! use krabnet::wal::{WalWriter, WalReader};
//! use krabnet::types::{Event, NodeId, TypeId, Epoch};
//! use std::path::Path;
//!
//! let path = Path::new("krabnet-wal.bin");
//! let mut writer = WalWriter::new(path, 1000).unwrap();
//! writer.append(Epoch(1), &Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(10) }).unwrap();
//! writer.flush().unwrap();
//! drop(writer);
//!
//! let entries = WalReader::replay(path).unwrap();
//! assert_eq!(entries.len(), 1);
//! ```

use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::types::{EdgeId, Epoch, Event, NodeId, PropertyValue, TypeId};

/// Error type for WAL deserialization failures.
#[derive(Debug)]
pub enum WalError {
    /// An I/O error occurred.
    Io(std::io::Error),
    /// The WAL data is corrupted or has an unknown format.
    Corrupt(String),
}

impl From<std::io::Error> for WalError {
    fn from(err: std::io::Error) -> Self {
        WalError::Io(err)
    }
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {}", e),
            WalError::Corrupt(msg) => write!(f, "WAL corrupt: {}", msg),
        }
    }
}

impl std::error::Error for WalError {}

/// Serialize an [`Event`] to binary bytes.
///
/// Each variant is tagged with a single byte, followed by its fields
/// in little-endian format. This avoids adding external serialization
/// dependencies.
fn serialize_event(event: &Event) -> Vec<u8> {
    let mut buf = Vec::new();
    match event {
        Event::NodeAdded { node_id, type_id } => {
            buf.push(0u8);
            buf.extend_from_slice(&node_id.0.to_le_bytes());
            buf.extend_from_slice(&type_id.0.to_le_bytes());
        }
        Event::NodeRemoved { node_id } => {
            buf.push(1u8);
            buf.extend_from_slice(&node_id.0.to_le_bytes());
        }
        Event::EdgeAdded {
            edge_id,
            source,
            target,
            type_id,
        } => {
            buf.push(2u8);
            buf.extend_from_slice(&edge_id.0.to_le_bytes());
            buf.extend_from_slice(&source.0.to_le_bytes());
            buf.extend_from_slice(&target.0.to_le_bytes());
            buf.extend_from_slice(&type_id.0.to_le_bytes());
        }
        Event::EdgeRemoved {
            edge_id,
            source,
            target,
        } => {
            buf.push(3u8);
            buf.extend_from_slice(&edge_id.0.to_le_bytes());
            buf.extend_from_slice(&source.0.to_le_bytes());
            buf.extend_from_slice(&target.0.to_le_bytes());
        }
        Event::PropertyChanged {
            node_id,
            key,
            value,
        } => {
            buf.push(4u8);
            buf.extend_from_slice(&node_id.0.to_le_bytes());
            buf.extend_from_slice(&key.to_le_bytes());
            serialize_property_value(&mut buf, value);
        }
    }
    buf
}

/// Serialize a [`PropertyValue`] to the buffer with a sub-tag byte.
///
/// - Integer(i64): tag 0, 8 bytes
/// - Float(f64): tag 1, 8 bytes
/// - Text(u32): tag 2, 4 bytes
/// - Boolean(bool): tag 3, 1 byte
fn serialize_property_value(buf: &mut Vec<u8>, value: &PropertyValue) {
    match value {
        PropertyValue::Integer(v) => {
            buf.push(0u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::Float(v) => {
            buf.push(1u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::Text(v) => {
            buf.push(2u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::Boolean(v) => {
            buf.push(3u8);
            buf.push(if *v { 1u8 } else { 0u8 });
        }
    }
}

/// Deserialize an [`Event`] from a byte slice.
///
/// The first byte is the variant tag, followed by fields in the same
/// order as [`serialize_event`].
fn deserialize_event(data: &[u8]) -> Result<Event, WalError> {
    if data.is_empty() {
        return Err(WalError::Corrupt("empty event data".to_string()));
    }
    let tag = data[0];
    let rest = &data[1..];
    match tag {
        0 => {
            // NodeAdded: u64 node_id + u32 type_id
            if rest.len() < 12 {
                return Err(WalError::Corrupt("NodeAdded too short".to_string()));
            }
            let node_id = NodeId(u64::from_le_bytes(rest[0..8].try_into().unwrap()));
            let type_id = TypeId(u32::from_le_bytes(rest[8..12].try_into().unwrap()));
            Ok(Event::NodeAdded { node_id, type_id })
        }
        1 => {
            // NodeRemoved: u64 node_id
            if rest.len() < 8 {
                return Err(WalError::Corrupt("NodeRemoved too short".to_string()));
            }
            let node_id = NodeId(u64::from_le_bytes(rest[0..8].try_into().unwrap()));
            Ok(Event::NodeRemoved { node_id })
        }
        2 => {
            // EdgeAdded: u64 edge_id + u64 source + u64 target + u32 type_id
            if rest.len() < 28 {
                return Err(WalError::Corrupt("EdgeAdded too short".to_string()));
            }
            let edge_id = EdgeId(u64::from_le_bytes(rest[0..8].try_into().unwrap()));
            let source = NodeId(u64::from_le_bytes(rest[8..16].try_into().unwrap()));
            let target = NodeId(u64::from_le_bytes(rest[16..24].try_into().unwrap()));
            let type_id = TypeId(u32::from_le_bytes(rest[24..28].try_into().unwrap()));
            Ok(Event::EdgeAdded {
                edge_id,
                source,
                target,
                type_id,
            })
        }
        3 => {
            // EdgeRemoved: u64 edge_id + u64 source + u64 target
            if rest.len() < 24 {
                return Err(WalError::Corrupt("EdgeRemoved too short".to_string()));
            }
            let edge_id = EdgeId(u64::from_le_bytes(rest[0..8].try_into().unwrap()));
            let source = NodeId(u64::from_le_bytes(rest[8..16].try_into().unwrap()));
            let target = NodeId(u64::from_le_bytes(rest[16..24].try_into().unwrap()));
            Ok(Event::EdgeRemoved {
                edge_id,
                source,
                target,
            })
        }
        4 => {
            // PropertyChanged: u64 node_id + u32 key + PropertyValue
            if rest.len() < 12 {
                return Err(WalError::Corrupt("PropertyChanged too short".to_string()));
            }
            let node_id = NodeId(u64::from_le_bytes(rest[0..8].try_into().unwrap()));
            let key = u32::from_le_bytes(rest[8..12].try_into().unwrap());
            let value = deserialize_property_value(&rest[12..])?;
            Ok(Event::PropertyChanged {
                node_id,
                key,
                value,
            })
        }
        _ => Err(WalError::Corrupt(format!("unknown event tag: {}", tag))),
    }
}

/// Deserialize a [`PropertyValue`] from a byte slice.
fn deserialize_property_value(data: &[u8]) -> Result<PropertyValue, WalError> {
    if data.is_empty() {
        return Err(WalError::Corrupt("empty property value".to_string()));
    }
    let tag = data[0];
    let rest = &data[1..];
    match tag {
        0 => {
            if rest.len() < 8 {
                return Err(WalError::Corrupt("Integer value too short".to_string()));
            }
            Ok(PropertyValue::Integer(i64::from_le_bytes(
                rest[0..8].try_into().unwrap(),
            )))
        }
        1 => {
            if rest.len() < 8 {
                return Err(WalError::Corrupt("Float value too short".to_string()));
            }
            Ok(PropertyValue::Float(f64::from_le_bytes(
                rest[0..8].try_into().unwrap(),
            )))
        }
        2 => {
            if rest.len() < 4 {
                return Err(WalError::Corrupt("Text value too short".to_string()));
            }
            Ok(PropertyValue::Text(u32::from_le_bytes(
                rest[0..4].try_into().unwrap(),
            )))
        }
        3 => {
            if rest.is_empty() {
                return Err(WalError::Corrupt("Boolean value too short".to_string()));
            }
            Ok(PropertyValue::Boolean(rest[0] != 0))
        }
        _ => Err(WalError::Corrupt(format!(
            "unknown property value tag: {}",
            tag
        ))),
    }
}

/// Append-only WAL writer with configurable fsync interval.
///
/// Events are written in binary format: `[u32 length][u64 epoch][serialized event]`.
/// The length field stores the number of bytes after the length (i.e., 8 + event bytes).
/// After every `fsync_interval` appends, the writer flushes and fsyncs to disk.
pub struct WalWriter {
    file: BufWriter<std::fs::File>,
    fsync_interval: u64,
    entries_since_fsync: u64,
}

impl WalWriter {
    /// Create a new WAL writer at the given path.
    ///
    /// If the file exists, new entries are appended (not truncated).
    /// The `fsync_interval` controls how often the writer calls `fsync` --
    /// set to 1 for maximum durability, or higher for better throughput.
    pub fn new(path: &Path, fsync_interval: u64) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: BufWriter::new(file),
            fsync_interval,
            entries_since_fsync: 0,
        })
    }

    /// Append an event with its epoch to the WAL.
    ///
    /// Format: `[u32 length][u64 epoch][serialized event bytes]`
    /// The length is the total number of bytes AFTER the length field
    /// (epoch + event bytes).
    pub fn append(&mut self, epoch: Epoch, event: &Event) -> std::io::Result<()> {
        let event_bytes = serialize_event(event);
        let total_len = 8 + event_bytes.len(); // u64 epoch + event bytes
        self.file.write_all(&(total_len as u32).to_le_bytes())?;
        self.file.write_all(&epoch.0.to_le_bytes())?;
        self.file.write_all(&event_bytes)?;

        self.entries_since_fsync += 1;
        if self.entries_since_fsync >= self.fsync_interval {
            self.flush()?;
        }
        Ok(())
    }

    /// Explicitly flush and fsync the WAL to disk.
    ///
    /// Ensures all buffered writes are persisted to durable storage.
    /// Called automatically every `fsync_interval` appends, but can
    /// also be called manually for explicit durability guarantees
    /// (e.g., on graceful shutdown).
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        self.entries_since_fsync = 0;
        Ok(())
    }
}

/// WAL reader for crash recovery replay.
///
/// Reads all complete entries from a WAL file and returns them as
/// `(Epoch, Event)` pairs. Stops at the first incomplete or corrupted
/// entry, supporting crash recovery up to the last complete write.
pub struct WalReader;

impl WalReader {
    /// Replay all WAL entries from the given path.
    ///
    /// Returns a `Vec<(Epoch, Event)>` in write order. Stops at the first
    /// incomplete entry (UnexpectedEof), which indicates a crash boundary.
    /// This allows recovery of all events up to the last complete write.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or read (other
    /// than end-of-file conditions which are handled gracefully).
    pub fn replay(path: &Path) -> Result<Vec<(Epoch, Event)>, WalError> {
        let mut file = BufReader::new(std::fs::File::open(path)?);
        let mut entries = Vec::new();

        loop {
            // Read u32 length
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(WalError::Io(e)),
            }
            let total_len = u32::from_le_bytes(len_buf) as usize;

            // Read the full entry
            let mut entry_buf = vec![0u8; total_len];
            match file.read_exact(&mut entry_buf) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(WalError::Io(e)),
            }

            // Parse epoch (first 8 bytes)
            if entry_buf.len() < 8 {
                break; // Corrupted entry, stop replay
            }
            let epoch = Epoch(u64::from_le_bytes(entry_buf[0..8].try_into().unwrap()));
            let event = deserialize_event(&entry_buf[8..])?;
            entries.push((epoch, event));
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EdgeId, NodeId, TypeId};

    /// TEST-22: WAL write-and-replay roundtrip test.
    ///
    /// Creates a temporary WAL file, writes 1000 events via WalWriter,
    /// drops the writer, replays via WalReader, and verifies all 1000
    /// (epoch, event) pairs match exactly.
    #[test]
    fn test_wal_write_and_replay() {
        let dir = std::env::temp_dir().join("krabnet_wal_test_roundtrip");
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test_roundtrip.wal");
        let _ = std::fs::remove_file(&wal_path);

        // Generate and write 1000 events
        let mut events: Vec<(Epoch, Event)> = Vec::new();
        {
            let mut writer = WalWriter::new(&wal_path, 100).unwrap();
            for i in 0..1000u64 {
                let epoch = Epoch(i + 1);
                let event = match i % 5 {
                    0 => Event::NodeAdded {
                        node_id: NodeId(i),
                        type_id: TypeId((i % 256) as u32),
                    },
                    1 => Event::NodeRemoved {
                        node_id: NodeId(i),
                    },
                    2 => Event::EdgeAdded {
                        edge_id: EdgeId(i),
                        source: NodeId(i),
                        target: NodeId(i + 1),
                        type_id: TypeId((i % 256) as u32),
                    },
                    3 => Event::EdgeRemoved {
                        edge_id: EdgeId(i),
                        source: NodeId(i),
                        target: NodeId(i + 1),
                    },
                    4 => Event::PropertyChanged {
                        node_id: NodeId(i),
                        key: (i % 100) as u32,
                        value: match i % 4 {
                            0 => PropertyValue::Integer(i as i64),
                            1 => PropertyValue::Float(i as f64 * 1.5),
                            2 => PropertyValue::Text((i % 1000) as u32),
                            _ => PropertyValue::Boolean(i % 2 == 0),
                        },
                    },
                    _ => unreachable!(),
                };
                writer.append(epoch, &event).unwrap();
                events.push((epoch, event));
            }
            writer.flush().unwrap();
        } // writer dropped here

        // Replay and verify all events
        let replayed = WalReader::replay(&wal_path).unwrap();
        assert_eq!(replayed.len(), 1000, "should replay all 1000 events");

        for (i, ((orig_epoch, orig_event), (rep_epoch, rep_event))) in
            events.iter().zip(replayed.iter()).enumerate()
        {
            assert_eq!(
                orig_epoch, rep_epoch,
                "epoch mismatch at index {}",
                i
            );
            assert_eq!(
                orig_event, rep_event,
                "event mismatch at index {}",
                i
            );
        }

        // Cleanup
        let _ = std::fs::remove_file(&wal_path);
        let _ = std::fs::remove_dir(&dir);
    }

    /// TEST-23: WAL crash recovery test.
    ///
    /// Writes 1000 events, truncates the file to simulate a crash (partial
    /// write), and verifies that replay recovers all complete entries up
    /// to the truncation point.
    #[test]
    fn test_wal_crash_recovery() {
        let dir = std::env::temp_dir().join("krabnet_wal_test_crash");
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test_crash.wal");
        let _ = std::fs::remove_file(&wal_path);

        // Write 1000 events with flush
        {
            let mut writer = WalWriter::new(&wal_path, 100).unwrap();
            for i in 0..1000u64 {
                let event = Event::NodeAdded {
                    node_id: NodeId(i),
                    type_id: TypeId((i % 256) as u32),
                };
                writer.append(Epoch(i + 1), &event).unwrap();
            }
            writer.flush().unwrap();
        }

        // Read the complete file size
        let full_size = std::fs::metadata(&wal_path).unwrap().len();
        assert!(full_size > 0, "WAL file should not be empty");

        // Truncate 5 bytes off the end to simulate a partial write crash
        let truncated_size = full_size - 5;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.set_len(truncated_size).unwrap();
        drop(file);

        // Replay should recover all but the last incomplete entry
        let replayed = WalReader::replay(&wal_path).unwrap();
        assert!(
            replayed.len() >= 998,
            "should recover at least 998 of 1000 events, got {}",
            replayed.len()
        );
        assert!(
            replayed.len() < 1000,
            "should not recover all 1000 (file was truncated), got {}",
            replayed.len()
        );

        // Verify recovered entries are correct
        for (i, (epoch, event)) in replayed.iter().enumerate() {
            assert_eq!(*epoch, Epoch(i as u64 + 1), "epoch mismatch at {}", i);
            let expected = Event::NodeAdded {
                node_id: NodeId(i as u64),
                type_id: TypeId((i as u64 % 256) as u32),
            };
            assert_eq!(*event, expected, "event mismatch at {}", i);
        }

        // Cleanup
        let _ = std::fs::remove_file(&wal_path);
        let _ = std::fs::remove_dir(&dir);
    }
}
