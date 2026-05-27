use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::process;

use raft_engine::{Config, Engine};
use raft_engine_strings::{
    decode_wal_entry_json, decode_wal_entry_pretty_json, encode_hex, extract_log_store_entry_data,
    extract_raft_entry_data, extract_readable_strings,
};

enum Command {
    ListNamespace { path: String },
    InspectEntry(InspectEntryOpts),
}

struct InspectEntryOpts {
    path: String,
    namespace: u64,
    entry_id: Option<u64>,
    min_entry_id: Option<u64>,
    max_entry_id: Option<u64>,
    min_len: usize,
    output: Option<String>,
    raw: bool,
    json: bool,
}

enum OutputRecord {
    EntryRange(Option<(u64, u64)>),
    Field {
        entry_id: u64,
        field: String,
        precise: bool,
        content: String,
    },
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let command = parse_args(env::args().skip(1))?;
    match command {
        Command::ListNamespace { path } => run_list_namespace(path),
        Command::InspectEntry(opts) => run_inspect_entry(opts),
    }
}

fn run_list_namespace(path: String) -> Result<(), String> {
    let engine = Engine::open(Config {
        dir: path,
        ..Default::default()
    })
    .map_err(|e| format!("failed to open raft engine: {e}"))?;

    let mut namespaces = engine.raft_groups();
    namespaces.sort_unstable();

    let stdout = io::stdout();
    let mut writer = stdout.lock();
    for namespace in namespaces {
        writeln!(writer, "{namespace}").map_err(|e| format!("failed to write output: {e}"))?;
    }
    writer
        .flush()
        .map_err(|e| format!("failed to flush output: {e}"))?;
    Ok(())
}

fn run_inspect_entry(opts: InspectEntryOpts) -> Result<(), String> {
    let engine = Engine::open(Config {
        dir: opts.path.clone(),
        ..Default::default()
    })
    .map_err(|e| format!("failed to open raft engine: {e}"))?;

    let stdout = io::stdout();
    let mut writer: Box<dyn Write> = if let Some(output) = opts.output {
        Box::new(BufWriter::new(
            File::create(&output).map_err(|e| format!("failed to create {output}: {e}"))?,
        ))
    } else {
        Box::new(stdout.lock())
    };

    let actual_entry_range = match (
        engine.first_index(opts.namespace),
        engine.last_index(opts.namespace),
    ) {
        (Some(first), Some(last)) => Some((first, last)),
        _ => None,
    };
    let entry_range = filter_entry_range(
        actual_entry_range,
        opts.entry_id,
        opts.min_entry_id,
        opts.max_entry_id,
    );
    let mut records = Vec::new();
    records.push(OutputRecord::EntryRange(entry_range));
    if let Some((first, last)) = entry_range {
        collect_raft_entries(
            &mut records,
            &engine,
            opts.namespace,
            first,
            last,
            opts.min_len,
            opts.raw,
        )?;
    }

    if opts.entry_id.is_none() && opts.min_entry_id.is_none() && opts.max_entry_id.is_none() {
        let mut entry_id = 0;

        engine
            .scan_raw_messages(opts.namespace, None, None, false, |key, value| {
                entry_id += 1;
                for (field, bytes) in [("key", key), ("value", value)] {
                    records.push(field_record(entry_id, field, bytes, opts.min_len));
                }
                true
            })
            .map_err(|e| format!("failed to scan namespace {}: {e}", opts.namespace))?;
    }

    if opts.json {
        write_json(&mut writer, &records).map_err(|e| format!("failed to write output: {e}"))?;
    } else {
        write_plain_text(&mut writer, opts.namespace, &records)
            .map_err(|e| format!("failed to write output: {e}"))?;
    }
    writer
        .flush()
        .map_err(|e| format!("failed to flush output: {e}"))?;

    Ok(())
}

fn filter_entry_range(
    actual: Option<(u64, u64)>,
    entry_id: Option<u64>,
    min_entry_id: Option<u64>,
    max_entry_id: Option<u64>,
) -> Option<(u64, u64)> {
    let (mut first, mut last) = actual?;
    if let Some(entry_id) = entry_id {
        first = entry_id;
        last = entry_id;
    }
    if let Some(min_entry_id) = min_entry_id {
        first = first.max(min_entry_id);
    }
    if let Some(max_entry_id) = max_entry_id {
        last = last.min(max_entry_id);
    }
    (first <= last).then_some((first, last))
}

fn collect_raft_entries(
    records: &mut Vec<OutputRecord>,
    engine: &Engine,
    namespace: u64,
    first: u64,
    last: u64,
    min_len: usize,
    raw: bool,
) -> Result<(), String> {
    for index in first..=last {
        let Some(raw_entry) = engine
            .get_entry_bytes(namespace, index)
            .map_err(|e| format!("failed to fetch raft entry {index}: {e}"))?
        else {
            continue;
        };
        records.push(entry_field_record(index, &raw_entry, min_len, raw));
    }
    Ok(())
}

fn entry_field_record(entry_id: u64, raw_entry: &[u8], min_len: usize, raw: bool) -> OutputRecord {
    if let Some(data) =
        extract_log_store_entry_data(raw_entry).or_else(|| extract_raft_entry_data(raw_entry))
    {
        let json = if raw {
            decode_wal_entry_json(data)
        } else {
            decode_wal_entry_pretty_json(data)
        };
        if let Some(json) = json {
            return OutputRecord::Field {
                entry_id,
                field: "entry".to_owned(),
                precise: true,
                content: json,
            };
        }
    }
    field_record(entry_id, "entry", raw_entry, min_len)
}

fn field_record(entry_id: u64, field: &str, bytes: &[u8], min_len: usize) -> OutputRecord {
    let strings = extract_readable_strings(bytes, min_len);
    if strings.is_empty() {
        return OutputRecord::Field {
            entry_id,
            field: format!("{field}_hex"),
            precise: false,
            content: encode_hex(bytes),
        };
    }
    OutputRecord::Field {
        entry_id,
        field: field.to_owned(),
        precise: false,
        content: strings.join(" | "),
    }
}

fn write_plain_text(
    writer: &mut dyn Write,
    namespace: u64,
    records: &[OutputRecord],
) -> io::Result<()> {
    let range = records.iter().find_map(|record| match record {
        OutputRecord::EntryRange(range) => Some(*range),
        OutputRecord::Field { .. } => None,
    });
    match range.flatten() {
        Some((first, last)) => writeln!(
            writer,
            "namespace: {namespace}, entry id range: {first} ~ {last}"
        )?,
        None => writeln!(writer, "namespace: {namespace}, entry id range: none")?,
    }

    for record in records {
        if let OutputRecord::Field {
            entry_id,
            field,
            precise,
            content,
        } = record
        {
            writeln!(writer, "Entry ID:     {entry_id}")?;
            writeln!(writer, "Message type: {field}")?;
            writeln!(writer, "Precise:      {precise}")?;
            writeln!(writer, "Content:")?;
            writeln!(writer, "{content}")?;
            writeln!(writer, "---")?;
        }
    }
    Ok(())
}

fn write_json(writer: &mut dyn Write, records: &[OutputRecord]) -> io::Result<()> {
    let values = records
        .iter()
        .map(|record| match record {
            OutputRecord::EntryRange(Some((first, last))) => serde_json::json!({
                "type": "entry_range",
                "range": { "first": first, "last": last },
            }),
            OutputRecord::EntryRange(None) => serde_json::json!({
                "type": "entry_range",
                "range": null,
            }),
            OutputRecord::Field {
                entry_id,
                field,
                precise,
                content,
            } => serde_json::json!({
                "type": "field",
                "entry_id": entry_id,
                "field": field,
                "precise": precise,
                "content": content,
            }),
        })
        .collect::<Vec<_>>();
    serde_json::to_writer_pretty(writer, &values)?;
    Ok(())
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Command, String> {
    let mut args = args.peekable();
    let Some(command) = args.next() else {
        return Err(usage());
    };

    match command.as_str() {
        "list-namespace" => parse_list_namespace_args(args),
        "inspect-entry" => parse_inspect_entry_args(args),
        "-h" | "--help" => Err(usage()),
        _ => Err(format!("unknown subcommand: {command}\n{}", usage())),
    }
}

fn parse_list_namespace_args(args: impl Iterator<Item = String>) -> Result<Command, String> {
    let mut path = None;
    let mut args = args.peekable();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-p" | "--path" => path = Some(next_value(&mut args, &arg, list_namespace_usage())?),
            "-h" | "--help" => return Err(list_namespace_usage()),
            _ => {
                return Err(format!(
                    "unknown argument: {arg}\n{}",
                    list_namespace_usage()
                ));
            }
        }
    }

    Ok(Command::ListNamespace {
        path: path.ok_or_else(list_namespace_usage)?,
    })
}

fn parse_inspect_entry_args(args: impl Iterator<Item = String>) -> Result<Command, String> {
    let mut path = None;
    let mut namespace = None;
    let mut entry_id = None;
    let mut min_entry_id = None;
    let mut max_entry_id = None;
    let mut min_len = 4;
    let mut output = None;
    let mut raw = false;
    let mut json = false;
    let mut args = args.peekable();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-p" | "--path" => path = Some(next_value(&mut args, &arg, inspect_entry_usage())?),
            "-n" | "--namespace" => {
                namespace = Some(parse_u64_value(
                    next_value(&mut args, &arg, inspect_entry_usage())?,
                    "namespace",
                )?)
            }
            "--entry-id" => {
                entry_id = Some(parse_u64_value(
                    next_value(&mut args, &arg, inspect_entry_usage())?,
                    "entry-id",
                )?)
            }
            "--min-entry-id" => {
                min_entry_id = Some(parse_u64_value(
                    next_value(&mut args, &arg, inspect_entry_usage())?,
                    "min-entry-id",
                )?)
            }
            "--max-entry-id" => {
                max_entry_id = Some(parse_u64_value(
                    next_value(&mut args, &arg, inspect_entry_usage())?,
                    "max-entry-id",
                )?)
            }
            "--min-len" => {
                min_len = next_value(&mut args, &arg, inspect_entry_usage())?
                    .parse()
                    .map_err(|_| "min-len must be an unsigned integer".to_owned())?
            }
            "-o" | "--output" => output = Some(next_value(&mut args, &arg, inspect_entry_usage())?),
            "--raw" => raw = true,
            "--json" => json = true,
            "-h" | "--help" => return Err(inspect_entry_usage()),
            _ => {
                return Err(format!(
                    "unknown argument: {arg}\n{}",
                    inspect_entry_usage()
                ));
            }
        }
    }

    Ok(Command::InspectEntry(InspectEntryOpts {
        path: path.ok_or_else(inspect_entry_usage)?,
        namespace: namespace.ok_or_else(inspect_entry_usage)?,
        entry_id,
        min_entry_id,
        max_entry_id,
        min_len,
        output,
        raw,
        json,
    }))
}

fn parse_u64_value(value: String, name: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{name} must be a u64\n{}", inspect_entry_usage()))
}

fn next_value(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
    usage: String,
) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}\n{usage}"))
}

fn usage() -> String {
    "usage: wal-reader <SUBCOMMAND> [OPTIONS]\nsubcommands:\n  list-namespace\n  inspect-entry"
        .to_owned()
}

fn list_namespace_usage() -> String {
    "usage: wal-reader list-namespace --path <RAFT_ENGINE_DIR>".to_owned()
}

fn inspect_entry_usage() -> String {
    "usage: wal-reader inspect-entry --path <RAFT_ENGINE_DIR> --namespace <U64> [--entry-id <U64>] [--min-entry-id <U64>] [--max-entry-id <U64>] [--min-len <N>] [--output <FILE>] [--raw] [--json]"
        .to_owned()
}
