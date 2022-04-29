extern crate docopt;
extern crate histogram;
extern crate rand;
extern crate regex;
extern crate rustc_serialize;
extern crate time;
extern crate wal;

use std::path::Path;
use std::str::FromStr;

use docopt::Docopt;
use histogram::Histogram;
use rand::Rng;
use regex::Regex;

use wal::Segment;

static USAGE: &'static str = "
Usage:
  bench append [--batch=<n>] [--segment-size=<ss>] [--entry-size=<es>] <segment-path>

Options:
  --batch=<n>           Sync entries to disk in batches of size n, or 0 for async [default: 1].
  --segment-size=<ss>   Segment size (bytes) [default: 100MiB]
  --entry-size=<es>     Entry size (bytes) [default: 1KiB]
  -h --help             Show a help message.
";

#[derive(Debug, RustcDecodable)]
struct Args {
    cmd_append: bool,

    arg_segment_path: String,

    flag_batch: usize,
    flag_entry_size: String,
    flag_segment_size: String,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());
    if args.cmd_append {
        append(&args);
    }
}

fn format_duration(n: u64) -> String {
    if n > 1_000_000_000 {
        format!("{:.2}s", n as f64 / 1_000_000_000f64)
    } else if n > 1_000_000 {
        format!("{:.2}ms", n as f64 / 1_000_000f64)
    } else if n > 1_000 {
        format!("{:.2}μs", n as f64 / 1_000f64)
    } else {
        format!("{}ns", n)
    }
}

fn format_bytes(n: usize) -> String {
    if n > 1_073_741_824 {
        format!("{:.2}GiB", n as f64 / 1_073_741_824f64)
    } else if n > 1_048_576 {
        format!("{:.2}MiB", n as f64 / 1_048_576f64)
    } else if n > 1_024 {
        format!("{:.2}KiB", n as f64 / 1_024f64)
    } else {
        format!("{}B", n)
    }
}

fn parse_bytes(s: &str) -> usize {
    let regex = Regex::new(r"(?i)^(\d+(?:\.\d+)?)\s?(k|m|g)?i?b?$").unwrap();
    let caps = regex
        .captures(s)
        .expect(&format!("unable to parse byte amount: {}", s));
    let n: usize = FromStr::from_str(caps.at(1).unwrap()).unwrap();

    match caps.at(2) {
        None => n,
        Some("k") | Some("K") => n * 1_024,
        Some("m") | Some("M") => n * 1_048_576,
        Some("g") | Some("G") => n * 1_073_741_824,
        _ => panic!("unable to parse byte amount: {}", s),
    }
}

fn append(args: &Args) {
    let path = Path::new(&args.arg_segment_path);

    let entry_size = parse_bytes(&args.flag_entry_size);
    let segment_size = parse_bytes(&args.flag_segment_size);

    println!(
        "entry size: {}, segment size: {}, batch size: {}",
        format_bytes(entry_size),
        format_bytes(segment_size),
        args.flag_batch
    );

    let mut segment = Segment::create(path, segment_size).unwrap();

    let mut buf = vec![0; entry_size as usize];
    rand::weak_rng().fill_bytes(&mut buf);

    let mut append_hist = Histogram::new().unwrap();
    let mut sync_hist = Histogram::new().unwrap();

    let mut entries = 0usize;
    let mut time: u64 = time::precise_time_ns();
    let start_time: u64 = time;
    while let Some(_) = segment.append(&buf) {
        entries += 1;
        if args.flag_batch != 0 && entries % args.flag_batch == 0 {
            let start_sync = time::precise_time_ns();
            //future.await().unwrap();
            sync_hist
                .increment(time::precise_time_ns() - start_sync)
                .unwrap();
        }
        let new_time = time::precise_time_ns();
        append_hist.increment(new_time - time).unwrap();
        time = new_time;
        rand::weak_rng().fill_bytes(&mut buf);
    }

    if args.flag_batch != 0 && entries % args.flag_batch != 0 {
        //segment.flush().await().unwrap();
        let new_time = time::precise_time_ns();
        append_hist.increment(new_time - time).unwrap();
    }

    let end_time = time::precise_time_ns();

    if args.flag_batch == 0 {
        //segment.flush().await().unwrap();
        let flush_time = time::precise_time_ns() - end_time;
        println!("final sync latency: {}", format_duration(flush_time));
    }

    let time = end_time - start_time;
    let data = entries * entry_size;
    let rate = (data as f64 / (time as f64 / 1_000_000_000f64)) as usize;
    let overhead_amount = segment.len() as usize - data;
    let overhead_rate = (overhead_amount as f64 / (time as f64 / 1_000_000_000f64)) as usize;

    println!(
        "time: {}, data: {} ({}), rate {}/s ({}/s), entries appended: {}",
        format_duration(time),
        format_bytes(data),
        format_bytes(overhead_amount),
        format_bytes(rate),
        format_bytes(overhead_rate),
        entries
    );
    println!(
        "append latency:\t\tp50: {:>7},\tp75: {:>7},\tp90: {:>7},\tp95: {:>7},\tp99: {:>7}",
        append_hist
            .percentile(0.5)
            .map(format_duration)
            .unwrap_or("na".to_string()),
        append_hist
            .percentile(0.75)
            .map(format_duration)
            .unwrap_or("na".to_string()),
        append_hist
            .percentile(0.90)
            .map(format_duration)
            .unwrap_or("na".to_string()),
        append_hist
            .percentile(0.95)
            .map(format_duration)
            .unwrap_or("na".to_string()),
        append_hist
            .percentile(0.99)
            .map(format_duration)
            .unwrap_or("na".to_string())
    );
    println!(
        "sync latency:\t\tp50: {:>7},\tp75: {:>7},\tp90: {:>7},\tp95: {:>7},\tp99: {:>7}",
        sync_hist
            .percentile(0.5)
            .map(format_duration)
            .unwrap_or("na".to_string()),
        sync_hist
            .percentile(0.75)
            .map(format_duration)
            .unwrap_or("na".to_string()),
        sync_hist
            .percentile(0.90)
            .map(format_duration)
            .unwrap_or("na".to_string()),
        sync_hist
            .percentile(0.95)
            .map(format_duration)
            .unwrap_or("na".to_string()),
        sync_hist
            .percentile(0.99)
            .map(format_duration)
            .unwrap_or("na".to_string())
    );
}
