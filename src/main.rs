// Rust translation of snapshot-finder.py
// Minimal single-file program. Not a byte-for-byte port but preserves behavior.
// Cargo dependencies (add to Cargo.toml):

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::blocking::{Client, Response};
use reqwest::header::LOCATION;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use threadpool::ThreadPool;

#[derive(Parser, Debug)]
#[command(author, version, about = "Solana snapshot finder - rust port", long_about = None)]
struct Args {
    #[arg(short = 't', long = "threads-count", default_value_t = 1000)]
    threads_count: usize,

    #[arg(
        short = 'r',
        long = "rpc_address",
        default_value = "https://api.mainnet-beta.solana.com"
    )]
    rpc_address: String,

    #[arg(long = "slot", default_value_t = 0)]
    slot: u64,

    #[arg(long = "version")]
    version: Option<String>,

    #[arg(long = "wildcard_version")]
    wildcard_version: Option<String>,

    #[arg(long = "max_snapshot_age", default_value_t = 1300)]
    max_snapshot_age: i64,

    #[arg(long = "min_download_speed", default_value_t = 60)]
    min_download_speed: u64,

    #[arg(long = "max_download_speed")]
    max_download_speed: Option<u64>,

    #[arg(long = "max_latency", default_value_t = 100)]
    max_latency: u64,

    #[arg(long = "with_private_rpc", default_value_t = false)]
    with_private_rpc: bool,

    #[arg(long = "measurement_time", default_value_t = 7)]
    measurement_time: u64,

    #[arg(long = "snapshot_path", default_value = ".")]
    snapshot_path: String,

    #[arg(long = "num_of_retries", default_value_t = 5)]
    num_of_retries: u32,

    #[arg(long = "sleep", default_value_t = 7)]
    sleep: u64,

    #[arg(long = "sort_order", default_value = "latency")]
    sort_order: String,

    #[arg(short = 'b', long = "blacklist", default_value = "")]
    blacklist: String,

    #[arg(short = 'v', long = "verbose", default_value_t = false)]
    verbose: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RpcNodeInfo {
    snapshot_address: String,
    slots_diff: i64,
    latency: f64,
    files_to_download: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct JsonData {
    last_update_at: f64,
    last_update_slot: u64,
    total_rpc_nodes: usize,
    rpc_nodes_with_actual_snapshot: usize,
    rpc_nodes: Vec<RpcNodeInfo>,
}

fn convert_size(size_bytes: u128) -> String {
    if size_bytes == 0 {
        return "0B".into();
    }
    let size_name = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    let i = (size_bytes as f64).log(1024.0).floor() as usize;
    let p = 1024u128.pow(i as u32);
    let s = (size_bytes as f64) / (p as f64);
    format!("{:.2} {}", s, size_name[i])
}

fn measure_speed(client: &Client, url: &str, measure_time: u64) -> Result<f64> {
    let full = format!("http://{}/snapshot.tar.bz2", url);
    let mut resp = client
        .get(&full)
        .timeout(Duration::from_secs((measure_time + 2) as u64))
        .send()?;
    resp.error_for_status_ref()?;

    let start = Instant::now();
    let mut last = start;
    let mut buf = [0u8; 81920];
    let mut loaded: usize = 0;
    let mut speeds: Vec<f64> = Vec::new();

    while start.elapsed().as_secs() < measure_time {
        let now = Instant::now();
        match resp.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                loaded += n;
                let delta = now.duration_since(last).as_secs_f64();
                if delta > 1.0 {
                    let estimated_bps = (loaded as f64) / delta;
                    speeds.push(estimated_bps);
                    last = now;
                    loaded = 0;
                }
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
    }
    if speeds.is_empty() {
        return Ok(0.0);
    }
    speeds.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = speeds[speeds.len() / 2];
    Ok(mid)
}

fn do_request(
    client: &Client,
    url: &str,
    method: &str,
    data: Option<&str>,
    timeout_secs: u64,
) -> Result<Response> {
    let req = match method.to_lowercase().as_str() {
        "get" => client.get(url),
        "post" => client.post(url).body(data.unwrap_or("".into()).to_string()),
        "head" => client.head(url),
        _ => client.get(url),
    };
    let resp = req.timeout(Duration::from_secs(timeout_secs)).send()?;
    Ok(resp)
}

fn get_current_slot(client: &Client, rpc: &str) -> Option<u64> {
    let d = r#"{"jsonrpc":"2.0","id":1, "method":"getSlot"}"#;
    match do_request(client, rpc, "post", Some(d), 25) {
        Ok(mut r) => match r.text() {
            Ok(text) => {
                if text.contains("result") {
                    match serde_json::from_str::<Value>(&text) {
                        Ok(v) => return v["result"].as_u64(),
                        Err(_) => return None,
                    }
                }
                None
            }
            Err(_) => None,
        },
        Err(_) => None,
    }
}

fn get_all_rpc_ips(
    client: &Client,
    rpc: &str,
    wildcard: &Option<String>,
    specific_version: &Option<String>,
    with_private: bool,
    ip_blacklist: &HashSet<String>,
    discarded_by_version: &mut usize,
) -> Result<Vec<String>> {
    let d = r#"{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}"#;
    let mut result_ips: Vec<String> = Vec::new();
    let resp = do_request(client, rpc, "post", Some(d), 25)?;
    let txt = resp.text()?;
    if !txt.contains("result") {
        anyhow::bail!("Can't get RPC ip addresses: {}", txt);
    }
    let v: Value = serde_json::from_str(&txt)?;
    if let Some(nodes) = v["result"].as_array() {
        for node in nodes {
            let version = node["version"].as_str().unwrap_or("");
            if (wildcard.is_some() && !version.contains(wildcard.as_ref().unwrap()))
                || (specific_version.is_some() && version != specific_version.as_ref().unwrap())
            {
                *discarded_by_version += 1;
                continue;
            }
            if let Some(rpc_field) = node["rpc"].as_str() {
                result_ips.push(rpc_field.to_string());
            } else if with_private {
                if let Some(gossip) = node["gossip"].as_str() {
                    if let Some(host) = gossip.split(':').next() {
                        result_ips.push(format!("{}:8899", host));
                    }
                }
            }
        }
    }
    // dedupe and filter blacklist
    result_ips.sort();
    result_ips.dedup();
    let filtered: Vec<String> = result_ips
        .into_iter()
        .filter(|ip| !ip_blacklist.contains(ip))
        .collect();
    Ok(filtered)
}

fn get_snapshot_slot(
    client: &Client,
    rpc_address: String,
    current_slot: u64,
    max_snapshot_age: i64,
    max_latency_ms: u64,
    full_local_snap_slot: Option<String>,
    json_nodes: Arc<Mutex<Vec<RpcNodeInfo>>>,
    pbar: Arc<ProgressBar>,
    discarded_counters: Arc<Mutex<(usize, usize, usize)>>,
) {
    // counters tuple: (arch_type, latency, slot)
    pbar.inc(1);
    let snap_url = format!("http://{}/snapshot.tar.bz2", rpc_address);
    let inc_url = format!("http://{}/incremental-snapshot.tar.bz2", rpc_address);

    // try incremental
    if let Ok(r) = do_request(client, &inc_url, "head", None, 1) {
        let elapsed_ms = r.elapsed().map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0);
        if elapsed_ms as u64 > max_latency_ms {
            let mut c = discarded_counters.lock().unwrap();
            c.1 += 1;
            return;
        }
        if let Some(loc) = r.headers().get(LOCATION) {
            if let Ok(loc_s) = loc.to_str() {
                if loc_s.ends_with(".tar") {
                    let mut c = discarded_counters.lock().unwrap();
                    c.0 += 1;
                    return;
                }
                let parts: Vec<&str> = loc_s.split('-').collect();
                if parts.len() >= 4 {
                    if let (Ok(incremental_snap_slot), Ok(snap_slot)) =
                        (parts[2].parse::<u64>(), parts[3].parse::<u64>())
                    {
                        let slots_diff = current_slot as i64 - snap_slot as i64;
                        if slots_diff < -100 {
                            let mut c = discarded_counters.lock().unwrap();
                            c.2 += 1;
                            return;
                        }
                        if slots_diff > max_snapshot_age {
                            let mut c = discarded_counters.lock().unwrap();
                            c.2 += 1;
                            return;
                        }
                        if full_local_snap_slot
                            .as_ref()
                            .map(|s| s.parse::<u64>().ok())
                            .flatten()
                            == Some(incremental_snap_slot)
                        {
                            let mut nodes = json_nodes.lock().unwrap();
                            nodes.push(RpcNodeInfo {
                                snapshot_address: rpc_address.clone(),
                                slots_diff,
                                latency: elapsed_ms,
                                files_to_download: vec![loc_s.to_string()],
                            });
                            return;
                        }
                        // try full
                        if let Ok(r2) = do_request(client, &snap_url, "head", None, 1) {
                            if let Some(loc2) = r2.headers().get(LOCATION) {
                                let mut nodes = json_nodes.lock().unwrap();
                                nodes.push(RpcNodeInfo {
                                    snapshot_address: rpc_address.clone(),
                                    slots_diff,
                                    latency: elapsed_ms,
                                    files_to_download: vec![
                                        loc_s.to_string(),
                                        loc2.to_str().unwrap_or("").to_string(),
                                    ],
                                });
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    // try full snapshot
    if let Ok(r) = do_request(client, &snap_url, "head", None, 1) {
        let elapsed_ms = r.elapsed().map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0);
        if let Some(loc) = r.headers().get(LOCATION) {
            if let Ok(loc_s) = loc.to_str() {
                if loc_s.ends_with(".tar") {
                    let mut c = discarded_counters.lock().unwrap();
                    c.0 += 1;
                    return;
                }
                // parse full snap slot from snapshot-<slot>-...
                let parts: Vec<&str> = loc_s.split('-').collect();
                if parts.len() >= 2 {
                    if let Ok(full_snap_slot_) = parts[1].parse::<u64>() {
                        let slots_diff_full = current_slot as i64 - full_snap_slot_ as i64;
                        if slots_diff_full <= max_snapshot_age
                            && elapsed_ms as u64 <= max_latency_ms
                        {
                            let mut nodes = json_nodes.lock().unwrap();
                            nodes.push(RpcNodeInfo {
                                snapshot_address: rpc_address.clone(),
                                slots_diff: slots_diff_full,
                                latency: elapsed_ms,
                                files_to_download: vec![loc_s.to_string()],
                            });
                            return;
                        }
                    }
                }
            }
        }
    }
}

fn download(
    wget_path: &str,
    url: &str,
    snapshot_path: &str,
    max_download_speed_mb: Option<u64>,
) -> Result<()> {
    let fname = url.rsplit('/').next().unwrap_or("snapshot.tar.bz2");
    let temp = format!("{}/tmp-{}", snapshot_path, fname);
    let out = if let Some(max_mb) = max_download_speed_mb {
        Command::new(wget_path)
            .arg("--progress=dot:giga")
            .arg(format!("--limit-rate={}M", max_mb))
            .arg("--trust-server-names")
            .arg(url)
            .arg(format!("-O{}", temp))
            .output()?
    } else {
        Command::new(wget_path)
            .arg("--progress=dot:giga")
            .arg("--trust-server-names")
            .arg(url)
            .arg(format!("-O{}", temp))
            .output()?
    };
    if !out.status.success() {
        anyhow::bail!("wget failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    let dest = format!("{}/{}", snapshot_path, fname);
    fs::rename(&temp, &dest).with_context(|| format!("renaming {} -> {}", temp, dest))?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.verbose {
        env_logger::Builder::from_default_env().init();
    } else {
        env_logger::init();
    }

    let client = Client::builder()
        .danger_accept_invalid_certs(false)
        .build()?;

    let mut full_local_snap_slot: Option<String> = None;
    let snapshot_path = if args.snapshot_path.ends_with('/') {
        args.snapshot_path.trim_end_matches('/').to_string()
    } else {
        args.snapshot_path.clone()
    };

    fs::create_dir_all(&snapshot_path)?;

    let wget_path = which::which("wget").context("wget not found; please install wget")?;

    let blacklist: Vec<&str> = if args.blacklist.is_empty() {
        vec![]
    } else {
        args.blacklist.split(',').collect()
    };
    let ip_blacklist: HashSet<String> = HashSet::new();

    let mut num_attempts = 1u32;

    let mut discarded_by_version = 0usize;

    println!("Version: 0.3.9");
    println!("RPC: {}", args.rpc_address);

    while num_attempts <= args.num_of_retries {
        let current_slot = if args.slot != 0 {
            args.slot
        } else {
            get_current_slot(&client, &args.rpc_address).unwrap_or(0)
        };
        println!(
            "Attempt number: {}. Current slot: {}",
            num_attempts, current_slot
        );

        num_attempts += 1;

        let mut discarded_counters = Arc::new(Mutex::new((0usize, 0usize, 0usize)));
        let json_nodes = Arc::new(Mutex::new(Vec::<RpcNodeInfo>::new()));

        let rpc_ips = match get_all_rpc_ips(
            &client,
            &args.rpc_address,
            &args.wildcard_version,
            &args.version,
            args.with_private_rpc,
            &ip_blacklist,
            &mut discarded_by_version,
        ) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("error fetching rpc nodes: {}", e);
                return Err(e);
            }
        };

        println!("RPC servers in total: {}", rpc_ips.len());

        // find local full snapshots
        let mut full_local_snapshots = glob::glob(&format!("{}/snapshot-*tar*", snapshot_path))?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        full_local_snapshots.sort();
        full_local_snapshots.reverse();
        if !full_local_snapshots.is_empty() {
            if let Some(fname) = full_local_snapshots[0].file_name().and_then(|s| s.to_str()) {
                // pattern snapshot-<slot>-...
                let parts: Vec<&str> = fname.split('-').collect();
                if parts.len() >= 2 {
                    full_local_snap_slot = Some(parts[1].to_string());
                    println!(
                        "Found full local snapshot {} | FULL_LOCAL_SNAP_SLOT={}",
                        fname, parts[1]
                    );
                }
            }
        }

        println!("Searching information about snapshots on all found RPCs");

        let pool = ThreadPool::new(args.threads_count);
        let pbar = Arc::new(ProgressBar::new(rpc_ips.len() as u64));
        pbar.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap(),
        );

        for rpc in rpc_ips.into_iter() {
            let client = client.clone();
            let json_nodes = Arc::clone(&json_nodes);
            let pbar = Arc::clone(&pbar);
            let discarded_counters = Arc::clone(&discarded_counters);
            let rpc_clone = rpc.clone();
            let full_local_snap_slot = full_local_snap_slot.clone();
            pool.execute(move || {
                get_snapshot_slot(
                    &client,
                    rpc_clone,
                    current_slot,
                    args.max_snapshot_age,
                    args.max_latency,
                    full_local_snap_slot,
                    json_nodes,
                    pbar,
                    discarded_counters,
                );
            });
        }
        pool.join();
        pbar.finish();

        let mut nodes = json_nodes.lock().unwrap();
        println!("Found suitable RPCs: {}", nodes.len());

        if nodes.is_empty() {
            println!("No snapshot nodes were found matching the given parameters");
            if num_attempts > args.num_of_retries {
                break;
            }
            thread::sleep(Duration::from_secs(args.sleep));
            continue;
        }

        // sort
        if args.sort_order == "latency" {
            nodes.sort_by(|a, b| a.latency.partial_cmp(&b.latency).unwrap());
        } else {
            nodes.sort_by(|a, b| a.slots_diff.cmp(&b.slots_diff));
        }

        // save json
        let json_data = JsonData {
            last_update_at: chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
            last_update_slot: current_slot,
            total_rpc_nodes: 0,
            rpc_nodes_with_actual_snapshot: nodes.len(),
            rpc_nodes: nodes.clone(),
        };
        let out_path = format!("{}/snapshot.json", snapshot_path);
        fs::write(&out_path, serde_json::to_string_pretty(&json_data)?)?;
        println!("All data is saved to json file - {}", out_path);

        // measure speeds and try download
        let mut unsuitable_servers: HashSet<String> = HashSet::new();
        let mut best_snapshot_node = String::new();
        let num_of_rpc_to_check = 15usize;

        for (i, rpc_node) in nodes.iter().enumerate() {
            if !blacklist.is_empty() {
                if blacklist
                    .iter()
                    .any(|b| rpc_node.files_to_download.iter().any(|p| p.contains(b)))
                {
                    println!("{}/{} BLACKLISTED --> {:?}", i + 1, nodes.len(), rpc_node);
                    continue;
                }
            }
            if unsuitable_servers.contains(&rpc_node.snapshot_address) {
                println!(
                    "Rpc node already unsuitable --> skip {}",
                    rpc_node.snapshot_address
                );
                continue;
            }
            println!(
                "{}/{} checking the speed {:?}",
                i + 1,
                nodes.len(),
                rpc_node
            );
            let down_speed_bytes =
                measure_speed(&client, &rpc_node.snapshot_address, args.measurement_time)?;
            let down_speed_mb = convert_size(down_speed_bytes as u128);
            if down_speed_bytes < (args.min_download_speed as f64) * 1e6 {
                println!("Too slow: {} {}", rpc_node.snapshot_address, down_speed_mb);
                unsuitable_servers.insert(rpc_node.snapshot_address.clone());
                continue;
            } else {
                for path in rpc_node.files_to_download.iter().rev() {
                    if path.starts_with("/snapshot-") {
                        let parts: Vec<&str> = path.split('-').collect();
                        if parts.len() >= 2
                            && full_local_snap_slot
                                .as_ref()
                                .map(|s| s == parts[1])
                                .unwrap_or(false)
                        {
                            continue;
                        }
                    }
                    let mut candidate = String::new();
                    if path.contains("incremental") {
                        if let Ok(r) = do_request(
                            &client,
                            &format!(
                                "http://{}/incremental-snapshot.tar.bz2",
                                rpc_node.snapshot_address
                            ),
                            "head",
                            None,
                            2,
                        ) {
                            if let Some(loc) = r.headers().get(LOCATION) {
                                candidate = format!(
                                    "http://{}{}",
                                    rpc_node.snapshot_address,
                                    loc.to_str().unwrap_or("")
                                );
                            } else {
                                candidate = format!("http://{}{}", rpc_node.snapshot_address, path);
                            }
                        }
                    } else {
                        candidate = format!("http://{}{}", rpc_node.snapshot_address, path);
                    }
                    println!("Downloading {} snapshot to {}", candidate, snapshot_path);
                    download(
                        wget_path.to_str().unwrap(),
                        &candidate,
                        &snapshot_path,
                        args.max_download_speed,
                    )?;
                }
                println!("Done");
                return Ok(());
            }
            if i + 1 > num_of_rpc_to_check {
                break;
            }
        }

        println!(
            "No snapshot nodes were found matching the given parameters. RETRY #{}",
            num_attempts
        );
        thread::sleep(Duration::from_secs(args.sleep));
    }

    anyhow::bail!("Could not find a suitable snapshot")
}
