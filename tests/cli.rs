use std::process::Command;

use greptime_proto::v1::{ColumnSchema, Mutation, Row, Rows, Value, WalEntry, value};
use prost::Message as ProstMessage;
use protobuf::reflect::MessageDescriptor;
use protobuf::wire_format::WireType;
use protobuf::{
    Clear, CodedInputStream, CodedOutputStream, Message, ProtobufResult, UnknownFields, rt,
};
use raft::eraftpb::Entry;
use raft_engine::{Config, Engine, LogBatch, MessageExt};

#[derive(Clone, Debug, Default, PartialEq)]
struct TestEntry {
    index: u64,
    unknown_fields: UnknownFields,
}

impl Clear for TestEntry {
    fn clear(&mut self) {
        self.index = 0;
        self.unknown_fields.clear();
    }
}

impl Message for TestEntry {
    fn descriptor(&self) -> &'static MessageDescriptor {
        unimplemented!()
    }

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, _is: &mut CodedInputStream) -> ProtobufResult<()> {
        Ok(())
    }

    fn write_to_with_cached_sizes(&self, _os: &mut CodedOutputStream) -> ProtobufResult<()> {
        Ok(())
    }

    fn compute_size(&self) -> u32 {
        0
    }

    fn get_cached_size(&self) -> u32 {
        0
    }

    fn get_unknown_fields(&self) -> &UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn new() -> Self {
        Self::default()
    }

    fn default_instance() -> &'static Self {
        unimplemented!()
    }
}

struct TestEntryExt;

impl MessageExt for TestEntryExt {
    type Entry = TestEntry;

    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}

struct EntryExt;

impl MessageExt for EntryExt {
    type Entry = Entry;

    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct LogStoreEntryImpl {
    id: u64,
    namespace_id: u64,
    data: Vec<u8>,
    unknown_fields: UnknownFields,
}

impl Clear for LogStoreEntryImpl {
    fn clear(&mut self) {
        self.id = 0;
        self.namespace_id = 0;
        self.data.clear();
        self.unknown_fields.clear();
    }
}

impl Message for LogStoreEntryImpl {
    fn descriptor(&self) -> &'static MessageDescriptor {
        unimplemented!()
    }

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, _is: &mut CodedInputStream) -> ProtobufResult<()> {
        Ok(())
    }

    fn write_to_with_cached_sizes(&self, os: &mut CodedOutputStream) -> ProtobufResult<()> {
        os.write_uint64(1, self.id)?;
        os.write_uint64(2, self.namespace_id)?;
        os.write_bytes(3, &self.data)?;
        os.write_unknown_fields(self.get_unknown_fields())
    }

    fn compute_size(&self) -> u32 {
        rt::value_size(1, self.id, WireType::WireTypeVarint)
            + rt::value_size(2, self.namespace_id, WireType::WireTypeVarint)
            + rt::bytes_size(3, &self.data)
            + rt::unknown_fields_size(self.get_unknown_fields())
    }

    fn get_cached_size(&self) -> u32 {
        self.compute_size()
    }

    fn get_unknown_fields(&self) -> &UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn new() -> Self {
        Self::default()
    }

    fn default_instance() -> &'static Self {
        unimplemented!()
    }
}

struct LogStoreEntryExt;

impl MessageExt for LogStoreEntryExt {
    type Entry = LogStoreEntryImpl;

    fn index(e: &Self::Entry) -> u64 {
        e.id
    }
}

#[test]
fn prints_strings_from_requested_namespace() {
    let dir = tempfile::Builder::new()
        .prefix("raft-engine-strings-cli")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg).unwrap();
    let mut batch = LogBatch::default();
    batch
        .put(
            42,
            b"visible-key".to_vec(),
            b"\x00hello namespace\xff".to_vec(),
        )
        .unwrap();
    batch
        .put(7, b"hidden-key".to_vec(), b"hidden namespace".to_vec())
        .unwrap();
    engine.write(&mut batch, true).unwrap();
    drop(engine);

    let output = Command::new(env!("CARGO_BIN_EXE_raft-engine-strings"))
        .args([
            "--path",
            dir.path().to_str().unwrap(),
            "--namespace",
            "42",
            "--min-len",
            "4",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("1\tkey\tfalse\tvisible-key"));
    assert!(stdout.contains("1\tvalue\tfalse\thello namespace"));
    assert!(!stdout.contains("hidden"));
}

#[test]
fn prints_entry_id_range_for_namespace() {
    let dir = tempfile::Builder::new()
        .prefix("raft-engine-strings-cli")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg).unwrap();
    let mut batch = LogBatch::default();
    let entries = [
        TestEntry {
            index: 10,
            ..Default::default()
        },
        TestEntry {
            index: 11,
            ..Default::default()
        },
    ];
    batch.add_entries::<TestEntryExt>(42, &entries).unwrap();
    batch
        .put(42, b"visible-key".to_vec(), b"hello namespace".to_vec())
        .unwrap();
    engine.write(&mut batch, true).unwrap();
    drop(engine);

    let output = Command::new(env!("CARGO_BIN_EXE_raft-engine-strings"))
        .args(["--path", dir.path().to_str().unwrap(), "--namespace", "42"])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("entry_range\t10\t11"));
}

#[test]
fn prints_raft_entry_data_from_entry_range() {
    let dir = tempfile::Builder::new()
        .prefix("raft-engine-strings-cli")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg).unwrap();
    let mut batch = LogBatch::default();
    let wal_entry = WalEntry {
        mutations: vec![Mutation {
            sequence: 42,
            ..Default::default()
        }],
        ..Default::default()
    };
    batch
        .add_entries::<EntryExt>(
            42,
            &[
                Entry {
                    index: 10,
                    data: b"first string\x00second string".to_vec().into(),
                    ..Default::default()
                },
                Entry {
                    index: 11,
                    data: vec![0xde, 0xad, 0xbe, 0xef].into(),
                    context: b"raw context payload".to_vec().into(),
                    ..Default::default()
                },
                Entry {
                    index: 12,
                    data: wal_entry.encode_to_vec().into(),
                    ..Default::default()
                },
            ],
        )
        .unwrap();
    engine.write(&mut batch, true).unwrap();
    drop(engine);

    let output = Command::new(env!("CARGO_BIN_EXE_raft-engine-strings"))
        .args(["--path", dir.path().to_str().unwrap(), "--namespace", "42"])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("10\tentry\tfalse\tfirst string | second string"));
    assert!(!stdout.contains("10\tentry\tfalse\tfirst string\n10\tentry\tfalse\tsecond string"));
    assert!(stdout.contains("11\tentry\tfalse\traw context payload"));
    assert!(stdout.contains("12\tentry\ttrue\t{"));
    assert!(stdout.contains("\"wal_entry\""));
    assert!(stdout.contains("sequence: 42"));
}

#[test]
fn decodes_greptimedb_log_store_entry_data_from_entry_range() {
    let dir = tempfile::Builder::new()
        .prefix("raft-engine-strings-cli")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg).unwrap();
    let wal_entry = WalEntry {
        mutations: vec![Mutation {
            sequence: 42,
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut batch = LogBatch::default();
    batch
        .add_entries::<LogStoreEntryExt>(
            42,
            &[LogStoreEntryImpl {
                id: 10,
                namespace_id: 42,
                data: wal_entry.encode_to_vec(),
                ..Default::default()
            }],
        )
        .unwrap();
    engine.write(&mut batch, true).unwrap();
    drop(engine);

    let output = Command::new(env!("CARGO_BIN_EXE_raft-engine-strings"))
        .args(["--path", dir.path().to_str().unwrap(), "--namespace", "42"])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("entry_range\t10\t10"));
    assert!(stdout.contains("10\tentry\ttrue\t{"));
    assert!(stdout.contains("\"wal_entry\""));
    assert!(stdout.contains("sequence: 42"));
}

#[test]
fn pretty_prints_wal_mutation_rows_as_json_objects() {
    let dir = tempfile::Builder::new()
        .prefix("raft-engine-strings-cli")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg).unwrap();
    let wal_entry = WalEntry {
        mutations: vec![Mutation {
            sequence: 42,
            rows: Some(Rows {
                schema: vec![
                    ColumnSchema {
                        column_name: "message".to_owned(),
                        ..Default::default()
                    },
                    ColumnSchema {
                        column_name: "timestamp".to_owned(),
                        ..Default::default()
                    },
                ],
                rows: vec![Row {
                    values: vec![
                        Value {
                            value_data: Some(value::ValueData::StringValue("hello".to_owned())),
                        },
                        Value {
                            value_data: Some(value::ValueData::TimestampNanosecondValue(
                                1_000_000_000,
                            )),
                        },
                    ],
                }],
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut batch = LogBatch::default();
    batch
        .add_entries::<LogStoreEntryExt>(
            42,
            &[LogStoreEntryImpl {
                id: 10,
                namespace_id: 42,
                data: wal_entry.encode_to_vec(),
                ..Default::default()
            }],
        )
        .unwrap();
    engine.write(&mut batch, true).unwrap();
    drop(engine);

    let output = Command::new(env!("CARGO_BIN_EXE_raft-engine-strings"))
        .args([
            "--path",
            dir.path().to_str().unwrap(),
            "--namespace",
            "42",
            "--pretty-print",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("10\tentry\ttrue\t{"));
    assert!(stdout.contains("\"rows\": ["));
    assert!(stdout.contains("\"message\": \"hello\""));
    assert!(stdout.contains("\"timestamp\": \"1970-01-01T00:00:01Z\""));
    assert!(!stdout.contains("\"schema\""));
    assert!(!stdout.contains("\"values\""));
}

#[test]
fn writes_strings_with_entry_id_to_output_file() {
    let dir = tempfile::Builder::new()
        .prefix("raft-engine-strings-cli")
        .tempdir()
        .unwrap();
    let output_path = dir.path().join("strings.tsv");
    let cfg = Config {
        dir: dir.path().join("engine").to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg).unwrap();
    let mut batch = LogBatch::default();
    batch
        .put(42, b"entry-key".to_vec(), b"entry content".to_vec())
        .unwrap();
    engine.write(&mut batch, true).unwrap();
    drop(engine);

    let output = Command::new(env!("CARGO_BIN_EXE_raft-engine-strings"))
        .args([
            "--path",
            dir.path().join("engine").to_str().unwrap(),
            "--namespace",
            "42",
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.is_empty());
    let file = std::fs::read_to_string(output_path).unwrap();
    assert!(file.contains("1\tkey\tfalse\tentry-key"));
    assert!(file.contains("1\tvalue\tfalse\tentry content"));
}

#[test]
fn prints_hex_when_field_has_no_readable_strings() {
    let dir = tempfile::Builder::new()
        .prefix("raft-engine-strings-cli")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg).unwrap();
    let mut batch = LogBatch::default();
    batch
        .put(42, vec![0x01, 0x02], vec![0xde, 0xad, 0xbe, 0xef])
        .unwrap();
    engine.write(&mut batch, true).unwrap();
    drop(engine);

    let output = Command::new(env!("CARGO_BIN_EXE_raft-engine-strings"))
        .args(["--path", dir.path().to_str().unwrap(), "--namespace", "42"])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("1\tkey_hex\tfalse\t0102"));
    assert!(stdout.contains("1\tvalue_hex\tfalse\tdeadbeef"));
}

#[test]
fn reports_namespace_as_u64() {
    let output = Command::new(env!("CARGO_BIN_EXE_raft-engine-strings"))
        .args(["--path", "unused", "--namespace", "-1"])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("namespace must be a u64"));
    assert!(stderr.contains("--namespace <U64>"));
}
