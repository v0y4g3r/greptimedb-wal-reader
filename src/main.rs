use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::process;

use raft_engine::{Config, Engine};
use raft_engine_strings::{
    decode_wal_entry_json, decode_wal_entry_pretty_json, encode_hex, extract_log_store_entry_data,
    extract_raft_entry_data, extract_readable_strings,
};

struct Opts {
    path: String,
    namespace: u64,
    min_len: usize,
    output: Option<String>,
    pretty_print: bool,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let opts = parse_args(env::args().skip(1))?;
    let engine = Engine::open(Config {
        dir: opts.path,
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

    let entry_range = write_entry_range(&mut writer, &engine, opts.namespace)
        .map_err(|e| format!("failed to write entry range: {e}"))?;
    if let Some((first, last)) = entry_range {
        write_raft_entries(
            &mut writer,
            &engine,
            opts.namespace,
            first,
            last,
            opts.min_len,
            opts.pretty_print,
        )?;
    }

    let mut entry_id = 0;
    let mut write_error = None;

    engine
        .scan_raw_messages(opts.namespace, None, None, false, |key, value| {
            entry_id += 1;
            for (field, bytes) in [("key", key), ("value", value)] {
                if let Err(e) = write_field(&mut writer, entry_id, field, bytes, opts.min_len) {
                    write_error = Some(e.to_string());
                    return false;
                }
            }
            true
        })
        .map_err(|e| format!("failed to scan namespace {}: {e}", opts.namespace))?;
    if let Some(e) = write_error {
        return Err(format!("failed to write output: {e}"));
    }
    writer
        .flush()
        .map_err(|e| format!("failed to flush output: {e}"))?;

    Ok(())
}

fn write_entry_range(
    writer: &mut dyn Write,
    engine: &Engine,
    namespace: u64,
) -> io::Result<Option<(u64, u64)>> {
    match (engine.first_index(namespace), engine.last_index(namespace)) {
        (Some(first), Some(last)) => {
            writeln!(writer, "entry_range\t{first}\t{last}")?;
            Ok(Some((first, last)))
        }
        _ => {
            writeln!(writer, "entry_range\tnone")?;
            Ok(None)
        }
    }
}

fn write_raft_entries(
    writer: &mut dyn Write,
    engine: &Engine,
    namespace: u64,
    first: u64,
    last: u64,
    min_len: usize,
    pretty_print: bool,
) -> Result<(), String> {
    for index in first..=last {
        let Some(raw_entry) = engine
            .get_entry_bytes(namespace, index)
            .map_err(|e| format!("failed to fetch raft entry {index}: {e}"))?
        else {
            continue;
        };
        write_entry_field(writer, index, &raw_entry, min_len, pretty_print)
            .map_err(|e| format!("failed to write raft entry {index}: {e}"))?;
    }
    Ok(())
}

fn write_entry_field(
    writer: &mut dyn Write,
    entry_id: u64,
    raw_entry: &[u8],
    min_len: usize,
    pretty_print: bool,
) -> io::Result<()> {
    if let Some(data) =
        extract_log_store_entry_data(raw_entry).or_else(|| extract_raft_entry_data(raw_entry))
    {
        let json = if pretty_print {
            decode_wal_entry_pretty_json(data)
        } else {
            decode_wal_entry_json(data)
        };
        if let Some(json) = json {
            writeln!(writer, "{entry_id}\tentry\ttrue\t{json}")?;
            return Ok(());
        }
    }
    write_field(writer, entry_id, "entry", raw_entry, min_len)
}

fn write_field(
    writer: &mut dyn Write,
    entry_id: u64,
    field: &str,
    bytes: &[u8],
    min_len: usize,
) -> io::Result<()> {
    let strings = extract_readable_strings(bytes, min_len);
    write_strings_or_hex(writer, entry_id, field, false, bytes, strings)
}

fn write_strings_or_hex(
    writer: &mut dyn Write,
    entry_id: u64,
    field: &str,
    precise: bool,
    bytes: &[u8],
    strings: Vec<String>,
) -> io::Result<()> {
    if strings.is_empty() {
        writeln!(
            writer,
            "{entry_id}\t{field}_hex\t{precise}\t{}",
            encode_hex(bytes)
        )?;
        return Ok(());
    }
    writeln!(
        writer,
        "{entry_id}\t{field}\t{precise}\t{}",
        strings.join(" | ")
    )?;
    Ok(())
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Opts, String> {
    let mut path = None;
    let mut namespace = None;
    let mut min_len = 4;
    let mut output = None;
    let mut pretty_print = false;
    let mut args = args.peekable();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-p" | "--path" => path = Some(next_value(&mut args, &arg)?),
            "-n" | "--namespace" => {
                namespace = Some(
                    next_value(&mut args, &arg)?
                        .parse()
                        .map_err(|_| format!("namespace must be a u64\n{}", usage()))?,
                )
            }
            "--min-len" => {
                min_len = next_value(&mut args, &arg)?
                    .parse()
                    .map_err(|_| "min-len must be an unsigned integer".to_owned())?
            }
            "-o" | "--output" => output = Some(next_value(&mut args, &arg)?),
            "--pretty-print" => pretty_print = true,
            "-h" | "--help" => return Err(usage()),
            _ => return Err(format!("unknown argument: {arg}\n{}", usage())),
        }
    }

    Ok(Opts {
        path: path.ok_or_else(usage)?,
        namespace: namespace.ok_or_else(usage)?,
        min_len,
        output,
        pretty_print,
    })
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}\n{}", usage()))
}

fn usage() -> String {
    "usage: raft-engine-strings --path <RAFT_ENGINE_DIR> --namespace <U64> [--min-len <N>] [--output <FILE>] [--pretty-print]"
        .to_owned()
}
