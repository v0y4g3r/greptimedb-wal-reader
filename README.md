# wal-reader

`wal-reader` is a small Rust CLI for inspecting `raft-engine` WAL directories used by GreptimeDB. It can list raft namespaces, inspect WAL entries for a namespace, and decode GreptimeDB WAL payloads into readable text or JSON.

The tool is useful when you have a local raft-engine WAL directory and need to understand which namespaces exist, which raft entry ids are present, or what row data GreptimeDB wrote into the WAL.

## Project Structure

- `Cargo.toml` - Rust package metadata, the `wal-reader` binary target, and dependencies including `raft-engine`, `greptime-proto`, and Arrow IPC/JSON decoding support.
- `src/main.rs` - CLI argument parsing, `list-namespace` and `inspect-entry` command handling, output formatting, and raft-engine scanning.
- `src/lib.rs` - reusable decoding helpers for printable strings, GreptimeDB log-store entry payloads, `WalEntry` mutations, and Arrow IPC bulk entries.
- `tests/cli.rs` - integration tests that create temporary raft-engine directories and validate CLI behavior.
- `assets/` - local sample WAL data and captured output used during development.

## Build

```sh
cargo build --release
```

The release binary is written to:

```sh
./target/release/wal-reader
```

## Commands

### List Namespaces

Print all raft namespaces found in a WAL directory, sorted ascending:

```sh
./target/release/wal-reader list-namespace --path /path/to/wal
```

Example output:

```text
4398046511104
5037996638208
```

### Inspect Entries

Inspect entries for one namespace:

```sh
./target/release/wal-reader inspect-entry --path /path/to/wal --namespace 4398046511104
```

The default output is human-readable plain text. It prints the namespace entry range and then one block per decoded field or raft entry.

```text
namespace: 4398046511104, entry id range: 1084 ~ 1085
Entry ID:     1084
Message type: entry
Precise:      true
Content:
{
  "wal_entry": {
    "mutations": [],
    "bulk_entries": []
  }
}
---
```

## Inspect Options

- `--entry-id <U64>` - inspect exactly one raft entry id.
- `--min-entry-id <U64>` - inspect entries at or after this raft entry id.
- `--max-entry-id <U64>` - inspect entries at or before this raft entry id.
- `--min-len <N>` - minimum printable string length for fallback string extraction; defaults to `4`.
- `--output <FILE>` or `-o <FILE>` - write output to a file instead of stdout.
- `--json` - emit JSON output records.
- `--raw` - render decoded `WalEntry` values with debug-style raw content instead of structured row JSON.

Examples:

```sh
./target/release/wal-reader inspect-entry \
  --path /path/to/wal \
  --namespace 4398046511104 \
  --entry-id 1084
```

```sh
./target/release/wal-reader inspect-entry \
  --path /path/to/wal \
  --namespace 4398046511104 \
  --min-entry-id 1084 \
  --max-entry-id 1085 \
  --json
```

```sh
./target/release/wal-reader inspect-entry \
  --path /path/to/wal \
  --namespace 4398046511104 \
  --output decoded.txt
```

## Decoding Behavior

`inspect-entry` first tries precise GreptimeDB WAL decoding:

- It unwraps GreptimeDB log-store `EntryImpl.data` payloads.
- It decodes `greptime_proto::v1::WalEntry` values.
- It renders mutation rows as JSON objects keyed by column name.
- It decodes bulk insert rows from Arrow IPC, including LZ4-compressed batches.
- It formats timestamp and date values into readable UTC/date strings when possible.

If precise decoding is not possible, the tool falls back to best-effort extraction:

- Printable strings are extracted from the raw bytes.
- If no printable strings meet `--min-len`, bytes are emitted as lowercase hex.
- Fallback records are marked with `Precise: false` in plain-text output or `"precise": false` in JSON output.

## Notes

- `--namespace` must be an unsigned 64-bit integer.
- Entry id filters apply to raft entries and suppress raw key/value namespace scans.
- Running multiple commands against the same raft-engine directory at the same time can fail because raft-engine takes a directory lock. Run commands sequentially for the same WAL path.
- `--json` currently emits output records where decoded content is stored as a string field.

## Development

Run the test suite:

```sh
cargo test
```

Check formatting:

```sh
cargo fmt --check
```
