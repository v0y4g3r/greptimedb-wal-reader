use arrow_flight::{FlightData, utils::flight_data_to_batches};
use arrow_json::{ArrayWriter, writer::WriterBuilder};
use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use greptime_proto::v1::{BulkWalEntry, OpType, Value, WalEntry, bulk_wal_entry, value};
use prost::Message;
use serde_json::{Map, Number};

pub fn extract_readable_strings(bytes: &[u8], min_len: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut i = 0;

    while i < bytes.len() {
        if let Some((ch, len)) = decode_readable_char(&bytes[i..]) {
            current.push(ch);
            i += len;
        } else {
            push_if_long_enough(&mut out, &mut current, min_len);
            i += 1;
        }
    }

    push_if_long_enough(&mut out, &mut current, min_len);
    out
}

pub fn extract_wal_entry_strings(bytes: &[u8], min_len: usize) -> Option<Vec<String>> {
    let entry = WalEntry::decode(bytes).ok()?;
    Some(extract_readable_strings(
        format!("{entry:?}").as_bytes(),
        min_len,
    ))
}

pub fn decode_wal_entry_json(bytes: &[u8]) -> Option<String> {
    let entry = WalEntry::decode(bytes).ok()?;
    serde_json::to_string(&serde_json::json!({ "wal_entry": format!("{entry:?}") })).ok()
}

pub fn decode_wal_entry_pretty_json(bytes: &[u8]) -> Option<String> {
    let entry = WalEntry::decode(bytes).ok()?;
    serde_json::to_string_pretty(&serde_json::json!({
        "wal_entry": {
            "mutations": entry.mutations.iter().map(mutation_to_json).collect::<Vec<_>>(),
            "bulk_entries": entry.bulk_entries.iter().map(bulk_entry_to_json).collect::<Vec<_>>(),
        }
    }))
    .ok()
}

fn bulk_entry_to_json(entry: &BulkWalEntry) -> serde_json::Value {
    serde_json::json!({
        "sequence": entry.sequence,
        "min_ts": entry.min_ts,
        "max_ts": entry.max_ts,
        "timestamp_index": entry.timestamp_index,
        "rows": decode_bulk_entry_rows(entry),
    })
}

fn decode_bulk_entry_rows(entry: &BulkWalEntry) -> serde_json::Value {
    let Some(bulk_wal_entry::Body::ArrowIpc(ipc)) = &entry.body else {
        return serde_json::json!({ "error": "missing Arrow IPC body" });
    };
    let flight_data = [
        FlightData::new().with_data_header(ipc.schema.clone()),
        FlightData::new()
            .with_data_header(ipc.data_header.clone())
            .with_data_body(ipc.payload.clone()),
    ];
    let Ok(batches) = flight_data_to_batches(&flight_data) else {
        return serde_json::json!({ "error": "failed to decode Arrow IPC rows" });
    };
    let batch_refs = batches.iter().collect::<Vec<_>>();
    let mut writer: ArrayWriter<Vec<u8>> = WriterBuilder::new()
        .with_explicit_nulls(true)
        .with_timestamp_format("%Y-%m-%dT%H:%M:%S%.fZ".to_owned())
        .build(Vec::new());
    if writer.write_batches(&batch_refs).is_err() || writer.finish().is_err() {
        return serde_json::json!({ "error": "failed to encode Arrow rows as JSON" });
    }
    serde_json::from_slice(writer.get_ref())
        .unwrap_or_else(|_| serde_json::json!({ "error": "failed to parse Arrow row JSON" }))
}

fn mutation_to_json(mutation: &greptime_proto::v1::Mutation) -> serde_json::Value {
    serde_json::json!({
        "op_type": OpType::try_from(mutation.op_type)
            .map(|op_type| op_type.as_str_name())
            .unwrap_or("UNKNOWN"),
        "sequence": mutation.sequence,
        "rows": mutation
            .rows
            .as_ref()
            .map(|rows| {
                rows.rows
                    .iter()
                    .map(|row| {
                        let mut object = Map::new();
                        for (schema, value) in rows.schema.iter().zip(&row.values) {
                            object.insert(schema.column_name.clone(), value_to_json(value));
                        }
                        serde_json::Value::Object(object)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    })
}

fn value_to_json(value: &Value) -> serde_json::Value {
    let Some(value_data) = &value.value_data else {
        return serde_json::Value::Null;
    };
    match value_data {
        value::ValueData::I8Value(v)
        | value::ValueData::I16Value(v)
        | value::ValueData::I32Value(v) => serde_json::json!(v),
        value::ValueData::I64Value(v) => serde_json::json!(v),
        value::ValueData::U8Value(v)
        | value::ValueData::U16Value(v)
        | value::ValueData::U32Value(v) => serde_json::json!(v),
        value::ValueData::U64Value(v) => serde_json::json!(v),
        value::ValueData::F32Value(v) => Number::from_f64(f64::from(*v))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        value::ValueData::F64Value(v) => Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        value::ValueData::BoolValue(v) => serde_json::json!(v),
        value::ValueData::BinaryValue(v) => serde_json::json!(encode_hex(v)),
        value::ValueData::StringValue(v) => serde_json::json!(v),
        value::ValueData::DateValue(v) => serde_json::json!(format_date(*v)),
        value::ValueData::DatetimeValue(v) => serde_json::json!(format_timestamp(*v, 1_000)),
        value::ValueData::TimestampSecondValue(v) => serde_json::json!(format_timestamp(*v, 1)),
        value::ValueData::TimestampMillisecondValue(v) => {
            serde_json::json!(format_timestamp(*v, 1_000))
        }
        value::ValueData::TimestampMicrosecondValue(v) => {
            serde_json::json!(format_timestamp(*v, 1_000_000))
        }
        value::ValueData::TimestampNanosecondValue(v) => {
            serde_json::json!(format_timestamp(*v, 1_000_000_000))
        }
        _ => serde_json::json!(format!("{value_data:?}")),
    }
}

fn format_date(days_since_epoch: i32) -> String {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|date| date.checked_add_signed(Duration::days(i64::from(days_since_epoch))))
        .map(|date| date.to_string())
        .unwrap_or_else(|| days_since_epoch.to_string())
}

fn format_timestamp(value: i64, units_per_second: i64) -> String {
    let secs = value.div_euclid(units_per_second);
    let subsecond_units = value.rem_euclid(units_per_second);
    let nanos = (subsecond_units * (1_000_000_000 / units_per_second)) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::AutoSi, true))
        .unwrap_or_else(|| value.to_string())
}

pub fn extract_raft_entry_data(bytes: &[u8]) -> Option<&[u8]> {
    extract_length_delimited_field(bytes, 4)
}

pub fn extract_log_store_entry_data(bytes: &[u8]) -> Option<&[u8]> {
    extract_length_delimited_field(bytes, 3)
}

fn extract_length_delimited_field(bytes: &[u8], target_field_number: u64) -> Option<&[u8]> {
    let mut i = 0;
    while i < bytes.len() {
        let (tag, tag_len) = read_varint(&bytes[i..])?;
        i += tag_len;

        let field_number = tag >> 3;
        let wire_type = tag & 0x07;
        match wire_type {
            0 => {
                let (_, len) = read_varint(&bytes[i..])?;
                i += len;
            }
            1 => i = i.checked_add(8)?,
            2 => {
                let (len, len_len) = read_varint(&bytes[i..])?;
                i += len_len;
                let len = usize::try_from(len).ok()?;
                let end = i.checked_add(len)?;
                if end > bytes.len() {
                    return None;
                }
                if field_number == target_field_number {
                    return Some(&bytes[i..end]);
                }
                i = end;
            }
            5 => i = i.checked_add(4)?,
            _ => return None,
        }
    }
    None
}

fn read_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0_u64;
    for (i, b) in bytes.iter().enumerate() {
        if i == 10 {
            return None;
        }
        value |= u64::from(b & 0x7f) << (i * 7);
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn decode_readable_char(bytes: &[u8]) -> Option<(char, usize)> {
    let first = *bytes.first()?;
    if first.is_ascii() {
        let ch = first as char;
        if ch.is_ascii_graphic() || ch == ' ' || ch == '\t' {
            return Some((ch, 1));
        }
        return None;
    }

    for len in 2..=4 {
        if bytes.len() < len {
            break;
        }
        if let Ok(s) = std::str::from_utf8(&bytes[..len]) {
            let mut chars = s.chars();
            if let Some(ch) = chars.next() {
                if chars.next().is_none() && !ch.is_control() {
                    return Some((ch, len));
                }
            }
        }
    }

    None
}

fn push_if_long_enough(out: &mut Vec<String>, current: &mut String, min_len: usize) {
    let trimmed = current.trim();
    if trimmed.chars().count() >= min_len {
        out.push(trimmed.to_owned());
    }
    current.clear();
}

#[cfg(test)]
mod tests {
    use super::{
        decode_wal_entry_json, decode_wal_entry_pretty_json, encode_hex,
        extract_log_store_entry_data, extract_raft_entry_data, extract_readable_strings,
        extract_wal_entry_strings,
    };
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    };
    use arrow_flight::{FlightData, SchemaAsIpc, utils::batches_to_flight_data};
    use arrow_ipc::CompressionType;
    use arrow_ipc::writer::{
        CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use greptime_proto::v1::{ArrowIpc, BulkWalEntry, Mutation, WalEntry, bulk_wal_entry};
    use prost::Message;

    #[test]
    fn extracts_printable_ascii_runs_from_binary_data() {
        let input = b"\x00\x01hello raft\x02\xffworld\nline";

        let strings = extract_readable_strings(input, 4);

        assert_eq!(strings, vec!["hello raft", "world", "line"]);
    }

    #[test]
    fn extracts_valid_utf8_words() {
        let input = b"prefix \xe6\x95\xb0\xe6\x8d\xae suffix";

        let strings = extract_readable_strings(input, 2);

        assert_eq!(strings, vec!["prefix 数据 suffix"]);
    }

    #[test]
    fn ignores_short_runs() {
        let input = b"ab\x00cde\x00long";

        let strings = extract_readable_strings(input, 4);

        assert_eq!(strings, vec!["long"]);
    }

    #[test]
    fn encodes_bytes_as_lowercase_hex() {
        assert_eq!(encode_hex(&[0x00, 0xab, 0xff]), "00abff");
    }

    #[test]
    fn extracts_raft_entry_data_field_without_validating_other_fields() {
        let bytes = b"\x08\xff\x01\x18\x0a\x22\x05hello";

        assert_eq!(extract_raft_entry_data(bytes), Some(&b"hello"[..]));
    }

    #[test]
    fn extracts_log_store_entry_data_field() {
        let bytes = b"\x08\x2a\x10\x07\x1a\x05hello";

        assert_eq!(extract_log_store_entry_data(bytes), Some(&b"hello"[..]));
    }

    #[test]
    fn decodes_wal_entry_before_extracting_strings() {
        let wal_entry = WalEntry {
            mutations: vec![Mutation {
                sequence: 42,
                ..Default::default()
            }],
            ..Default::default()
        };
        let bytes = wal_entry.encode_to_vec();

        let strings = extract_wal_entry_strings(&bytes, 4).unwrap();

        assert!(strings.iter().any(|s| s.contains("mutations")));
        assert!(strings.iter().any(|s| s.contains("sequence")));

        let json = decode_wal_entry_json(&bytes).unwrap();
        assert!(json.starts_with("{"));
        assert!(json.contains("\"wal_entry\""));
        assert!(json.contains("sequence: 42"));
    }

    #[test]
    fn decodes_bulk_wal_entry_rows_as_json() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["host-a", "host-b"])) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from(vec![
                    1_700_000_000_000,
                    1_700_000_001_000,
                ])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.5), None])) as ArrayRef,
            ],
        )
        .unwrap();
        let flight_data = batches_to_flight_data(schema.as_ref(), vec![batch]).unwrap();
        let wal_entry = WalEntry {
            bulk_entries: vec![BulkWalEntry {
                sequence: 10,
                min_ts: 1_700_000_000_000,
                max_ts: 1_700_000_001_000,
                timestamp_index: 1,
                body: Some(bulk_wal_entry::Body::ArrowIpc(ArrowIpc {
                    schema: flight_data[0].data_header.clone(),
                    data_header: flight_data[1].data_header.clone(),
                    payload: flight_data[1].data_body.clone(),
                })),
            }],
            ..Default::default()
        };

        let json = decode_wal_entry_pretty_json(&wal_entry.encode_to_vec()).unwrap();

        assert!(json.contains(r#""sequence": 10"#), "{json}");
        assert!(json.contains(r#""host": "host-a""#), "{json}");
        assert!(json.contains(r#""ts": "2023-11-14T22:13:20Z""#), "{json}");
        assert!(json.contains(r#""value": null"#), "{json}");
    }

    #[test]
    fn decodes_lz4_compressed_bulk_wal_entry_rows_as_json() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1_735_689_600_000])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
            ],
        )
        .unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .unwrap();
        let schema_data = FlightData::from(SchemaAsIpc::new(schema.as_ref(), &options));
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let mut compression_context = CompressionContext::default();
        let (dictionaries, batch_data) = data_gen
            .encode(
                &batch,
                &mut dictionary_tracker,
                &options,
                &mut compression_context,
            )
            .unwrap();
        assert!(dictionaries.is_empty());
        let batch_data = FlightData::from(batch_data);
        let wal_entry = WalEntry {
            bulk_entries: vec![BulkWalEntry {
                sequence: 10,
                min_ts: 1_735_689_600_000,
                max_ts: 1_735_689_600_000,
                timestamp_index: 0,
                body: Some(bulk_wal_entry::Body::ArrowIpc(ArrowIpc {
                    schema: schema_data.data_header,
                    data_header: batch_data.data_header,
                    payload: batch_data.data_body,
                })),
            }],
            ..Default::default()
        };

        let json = decode_wal_entry_pretty_json(&wal_entry.encode_to_vec()).unwrap();

        assert!(
            json.contains(r#""greptime_timestamp": "2025-01-01T00:00:00Z""#),
            "{json}"
        );
        assert!(json.contains(r#""greptime_value": 1.0"#), "{json}");
        assert!(!json.contains("failed to decode Arrow IPC rows"), "{json}");
    }
}
