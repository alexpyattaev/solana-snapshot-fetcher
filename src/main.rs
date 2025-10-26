use anyhow::{Context, Result};
use atomic_token_bucket::TokenBucket;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, trace};
use reqwest::redirect::Policy;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Solana snapshot finder - find and fetch solana snapshots",
    long_about = "This program will probe multiple RPC nodes, pick one with best download speed and
    fetch the snapshots from there. It will not fetch snapshots that are already present on your host."
)]
struct Args {
    #[arg(
        short = 'r',
        long = "rpc_address",
        default_value = "https://api.mainnet-beta.solana.com"
    )]
    /// Public RPC to use to fetch the gossip information
    rpc_address: String,

    #[arg(long, short)]
    /// Search for a snapshot with a specific slot number (useful for network restarts)
    /// If not provided, current slot is fetched from RPC.
    slot: Option<u64>,

    #[arg(long = "max_snapshot_age", default_value_t = 1300)]
    /// Maximum age of the snapshot relative to current slot to consider.
    max_snapshot_age: u64,

    #[arg(long = "min_download_speed", default_value_t = 60)]
    /// Minimal download speed, Mbps. If RPC is slower than this, it will be skipped.
    min_download_speed: u64,

    #[arg(long = "max_download_speed")]
    /// Maximal download speed, Mbps. Specifying this reduces load on the RPC node.
    max_download_speed: Option<u64>,

    #[arg(long = "max_latency", default_value_t = 200)]
    /// Maximal latency to RPC in ms. High latency is bad for download speed.
    max_latency: u64,

    #[arg(long = "with_private_rpc", default_value_t = false)]
    /// Enable adding and checking RPCs with the --private-rpc option.This slow down
    /// checking and searching but potentially increases the number of RPCs from which
    /// snapshots can be downloaded.
    with_private_rpc: bool,

    #[arg(long = "measurement_time", default_value_t = 3)]
    /// How long to fetch the snapshot for in order to measure speed
    measurement_time: u64,

    #[arg(long = "snapshot_path", default_value = ".")]
    /// Where to save snapshots
    snapshot_path: String,

    #[arg(long = "num_of_retries", default_value_t = 5)]
    num_of_retries: u32,

    #[arg(long = "sleep", default_value_t = 7)]
    /// Time to sleep between attempts if no valid RPCs are found
    sleep: u64,

    #[arg(long = "sort_order", default_value = "latency")]
    /// Sorting order. Can be "latency" or "slot_diff".
    sort_order: String,

    #[arg(short = 'b', long = "blacklist", default_value = "")]
    /// If the same corrupted archive is constantly downloaded, you can exclude it. Specify
    /// either the number of the slot you want to exclude, or the hash of the archive name.
    /// You can specify several, separated by commas. Example: -b 135501350,135501360 or
    /// --blacklist 135501350,some_hash
    blacklist: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RpcNodeInfo {
    snapshot_address: String,
    slots_diff: i64,
    latency_ms: u64,
    files_to_download: Vec<String>,
}

async fn measure_speed(url: &str, measure_time: u64) -> Result<f64> {
    let full = format!("http://{}/snapshot.tar.bz2", url);
    let client = Client::new();
    let mut resp = client
        .get(&full)
        .timeout(Duration::from_secs(measure_time + 2))
        .send()
        .await?;
    resp.error_for_status_ref()?;

    let start = Instant::now();
    let mut last = start;
    let mut loaded: usize = 0;
    let mut speeds: Vec<f64> = Vec::new();

    while start.elapsed().as_secs() < measure_time {
        let now = Instant::now();
        match resp.chunk().await {
            Ok(None) => break,
            Ok(Some(bytes)) => {
                loaded += bytes.len();
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

pub enum HttpRequest<'a> {
    Head,
    Post(&'a str),
}

impl<'a> HttpRequest<'a> {
    pub async fn send(self, url: &str, timeout_secs: u64) -> Result<Response> {
        let client = Client::builder().redirect(Policy::none()).build()?;

        let req = match self {
            HttpRequest::Head => client.head(url),
            HttpRequest::Post(body) => client
                .post(url)
                .body(body.to_owned())
                .header("Content-Type", "application/json"),
        };

        let resp = req
            .timeout(Duration::from_secs(timeout_secs))
            .send()
            .await?;

        Ok(resp)
    }
}

async fn get_current_slot(rpc: &str) -> Result<u64> {
    let d = r#"{"jsonrpc":"2.0","id":1, "method":"getSlot"}"#;
    let r = HttpRequest::Post(d).send(rpc, 5).await?;
    let text = r.text().await?;
    if text.contains("result") {
        let v = serde_json::from_str::<Value>(&text)?;
        v["result"]
            .as_u64()
            .context("Could not parse slot in RPC result")
    } else {
        anyhow::bail!("No result in returned json");
    }
}

async fn get_all_rpc_ips(
    rpc: &str,
    with_private: bool,
    ip_blacklist: &HashSet<String>,
) -> Result<Vec<String>> {
    let d = r#"{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}"#;
    let mut result_ips: Vec<String> = Vec::new();
    let resp = HttpRequest::Post(d).send(rpc, 5).await?;
    let txt = resp.text().await?;
    if !txt.contains("result") {
        anyhow::bail!("Can't get RPC ip addresses: {}", txt);
    }
    let v: Value = serde_json::from_str(&txt)?;
    if let Some(nodes) = v["result"].as_array() {
        for node in nodes {
            if let Some(rpc_field) = node["rpc"].as_str() {
                result_ips.push(rpc_field.to_string());
            } else if with_private
                && let Some(gossip) = node["gossip"].as_str()
                && let Some(host) = gossip.split(':').next()
            {
                result_ips.push(format!("{}:8899", host));
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

async fn get_snapshot_slot(
    rpc_address: String,
    full_local_snap_slot: u64,
    current_slot: u64,
    max_snapshot_age_in_slots: u64,
    max_latency_ms: u64,
) -> Option<RpcNodeInfo> {
    let url = format!("http://{rpc_address}/snapshot.tar.bz2");
    let inc_url = format!("http://{rpc_address}/incremental-snapshot.tar.bz2");

    let t0 = Instant::now();
    let inc_resp = match HttpRequest::Head.send(&inc_url, 3).await {
        Ok(r) => r,
        Err(e) => {
            trace!("RPC request error {e}");
            return None;
        }
    };
    let latency_ms = t0.elapsed().as_millis() as u64;

    if latency_ms > max_latency_ms {
        trace!("Latency for {rpc_address} too high ({latency_ms}ms)");
        return None;
    }

    if let Some(incremental_snap_location) = inc_resp
        .headers()
        .get("location")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned)
    {
        if incremental_snap_location.ends_with("tar") {
            trace!("{rpc_address}: incorrect file extension");
            return None;
        }

        let parts: Vec<&str> = incremental_snap_location.split('-').collect();
        if parts.len() < 4 {
            trace!("{rpc_address}: invalid path: {parts:?}");
            return None;
        }

        let incremental_snap_slot = parts[2].parse::<u64>().ok()?;
        let snap_slot = parts[3].parse::<u64>().ok()?;
        let slots_diff = current_slot as i64 - snap_slot as i64;

        if slots_diff < -100 {
            error!(
                "Invalid snapshot from node {}; slots_diff={}",
                rpc_address, slots_diff
            );
            return None;
        }

        if slots_diff > max_snapshot_age_in_slots as i64 {
            trace!("{rpc_address}: too old ({slots_diff} slots)");
            return None;
        }

        if full_local_snap_slot == incremental_snap_slot {
            return Some(RpcNodeInfo {
                snapshot_address: rpc_address.clone(),
                slots_diff,
                latency_ms,
                files_to_download: vec![incremental_snap_location],
            });
        }

        let full_resp = HttpRequest::Head.send(&url, 1).await.ok()?;

        if let Some(full_snap_location) = full_resp
            .headers()
            .get("location")
            .and_then(|h| h.to_str().ok())
            .map(str::to_owned)
        {
            return Some(RpcNodeInfo {
                snapshot_address: rpc_address.clone(),
                slots_diff,
                latency_ms,
                files_to_download: vec![incremental_snap_location, full_snap_location],
            });
        }
    } else {
        trace!("No location header from {rpc_address}");
        return None;
    }

    // check full snapshot if incremental didn't match
    let full_resp = HttpRequest::Head.send(&url, 1).await.ok()?;

    if let Some(full_snap_location) = full_resp
        .headers()
        .get("location")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned)
    {
        if full_snap_location.ends_with("tar") {
            trace!("{rpc_address}: invalid file format {full_snap_location}");
            return None;
        }

        let parts: Vec<&str> = full_snap_location.split('-').collect();
        if parts.len() < 2 {
            trace!("{rpc_address}: invalid path: {parts:?}");
            return None;
        }

        let full_snap_slot = parts[1].parse::<u64>().ok()?;
        let slots_diff_full = current_slot as i64 - full_snap_slot as i64;

        if slots_diff_full <= max_snapshot_age_in_slots as i64 {
            return Some(RpcNodeInfo {
                snapshot_address: rpc_address.clone(),
                slots_diff: slots_diff_full,
                latency_ms,
                files_to_download: vec![full_snap_location],
            });
        } else {
            trace!("{rpc_address}: too old ({slots_diff_full} slots)");
        }
    }

    trace!("No snapshots on {rpc_address}!");
    None
}

async fn download(
    url: &str,
    snapshot_path: &str,
    max_download_speed_mb: Option<u64>,
) -> Result<()> {
    let fname = url.rsplit('/').next().unwrap_or("snapshot.tar.bz2");
    let temp = format!("{}/tmp-{}", snapshot_path, fname);
    let dest = format!("{}/{}", snapshot_path, fname);

    let token_bucket = if let Some(max_download_speed_mb) = max_download_speed_mb {
        let max_speed_bytes_per_second = max_download_speed_mb * 125_000;
        Some(TokenBucket::new(
            max_speed_bytes_per_second,
            max_speed_bytes_per_second,
            max_speed_bytes_per_second as f64,
        ))
    } else {
        None
    };

    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;
    let mut resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("HTTP error: {}", resp.status());
    }
    let size: u64 = resp
        .headers()
        .get("content-length")
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let pbar = ProgressBar::new(size);
    pbar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
        )
        .unwrap(),
    );

    let mut file = tokio::fs::File::create(&temp).await?;

    while let Some(chunk) = resp.chunk().await? {
        file.write_all(&chunk).await?;
        pbar.inc(chunk.len() as u64);
        if let Some(token_bucket) = token_bucket.as_ref() {
            while token_bucket.consume_tokens(chunk.len() as u64).is_err() {
                sleep(Duration::from_millis(10)).await;
            }
        }
    }
    file.flush().await?;
    tokio::fs::rename(temp, dest).await?;
    pbar.finish();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::init();

    let mut full_local_snap_slot: u64 = 0;
    let snapshot_path = if args.snapshot_path.ends_with('/') {
        args.snapshot_path.trim_end_matches('/').to_string()
    } else {
        args.snapshot_path.clone()
    };

    fs::create_dir_all(&snapshot_path).await?;

    let blacklist: Vec<&str> = if args.blacklist.is_empty() {
        vec![]
    } else {
        args.blacklist.split(',').collect()
    };
    let ip_blacklist: HashSet<String> = HashSet::new();

    println!("RPC: {}", args.rpc_address);

    for num_attempts in 1..=args.num_of_retries {
        let current_slot = if let Some(current_slot) = args.slot {
            current_slot
        } else {
            get_current_slot(&args.rpc_address).await?
        };
        println!(
            "Attempt number: {}. Current slot: {}",
            num_attempts, current_slot
        );

        let rpc_ips =
            get_all_rpc_ips(&args.rpc_address, args.with_private_rpc, &ip_blacklist).await?;

        println!("RPC servers in total: {}", rpc_ips.len());

        // find local full snapshots
        let mut full_local_snapshots = find_full_local_snapshots(&snapshot_path)
            .await
            .with_context(|| format!("Error reading local snapshots from {snapshot_path}"))?;
        full_local_snapshots.sort();
        if let Some(fname) = full_local_snapshots.last() {
            // pattern snapshot-<slot>-...
            let parts: Vec<&str> = fname.split('-').collect();
            if parts.len() >= 2 {
                println!(
                    "Found full local snapshot {} | FULL_LOCAL_SNAP_SLOT={}",
                    fname, parts[1]
                );
                full_local_snap_slot = parts[1].parse()?;
            }
        }

        println!("Searching information about snapshots on all found RPCs");

        let pbar = Arc::new(ProgressBar::new(rpc_ips.len() as u64));
        pbar.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap(),
        );

        let mut join_set = tokio::task::JoinSet::new();
        for rpc in rpc_ips.into_iter() {
            let pbar = Arc::clone(&pbar);
            join_set.spawn(async move {
                let res = get_snapshot_slot(
                    rpc,
                    full_local_snap_slot,
                    current_slot,
                    args.max_snapshot_age,
                    args.max_latency,
                )
                .await;
                pbar.inc(1);
                res
            });
        }
        let mut nodes = vec![];
        while let Some(join_result) = join_set.join_next().await {
            if let Some(node) = join_result? {
                nodes.push(node);
            }
        }
        assert!(join_set.is_empty());

        pbar.finish();

        if nodes.is_empty() {
            println!("No snapshot nodes were found matching the given parameters");
            sleep(Duration::from_secs(args.sleep)).await;
            continue;
        }
        println!("Found suitable RPCs: {}", nodes.len());

        // sort
        if args.sort_order == "latency" {
            nodes.sort_by(|a, b| a.latency_ms.partial_cmp(&b.latency_ms).unwrap());
        } else {
            nodes.sort_by(|a, b| a.slots_diff.cmp(&b.slots_diff));
        }
        // measure speeds and try download
        let mut unsuitable_servers: HashSet<String> = HashSet::new();

        for (i, rpc_node) in nodes.iter().enumerate() {
            if !blacklist.is_empty()
                && blacklist
                    .iter()
                    .any(|b| rpc_node.files_to_download.iter().any(|p| p.contains(b)))
            {
                println!("{}/{} BLACKLISTED --> {:?}", i + 1, nodes.len(), rpc_node);
                continue;
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
                measure_speed(&rpc_node.snapshot_address, args.measurement_time).await?;
            if down_speed_bytes < (args.min_download_speed as f64) * 1e6 {
                println!(
                    "Too slow: {} {} Mbit/s",
                    rpc_node.snapshot_address,
                    down_speed_bytes / 1e6
                );
                unsuitable_servers.insert(rpc_node.snapshot_address.clone());
                continue;
            } else {
                for path in rpc_node.files_to_download.iter().rev() {
                    println!("Will download {path}");
                    let mut candidate = String::new();
                    if path.contains("incremental") {
                        if let Ok(r) = HttpRequest::Head
                            .send(
                                &format!(
                                    "http://{}/incremental-snapshot.tar.bz2",
                                    rpc_node.snapshot_address
                                ),
                                2,
                            )
                            .await
                        {
                            if let Some(loc) = r.headers().get("location") {
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
                    download(&candidate, &snapshot_path, args.max_download_speed).await?;
                }
                println!("Done");
                return Ok(());
            }
        }
        println!(
            "No snapshot nodes were found matching the given parameters. RETRY #{}",
            num_attempts + 1
        );
        sleep(Duration::from_secs(args.sleep)).await;
    }

    anyhow::bail!("Could not find a suitable snapshot")
}

pub async fn find_full_local_snapshots(snapshot_path: &str) -> Result<Vec<String>> {
    let mut entries = tokio::fs::read_dir(snapshot_path)
        .await
        .context(format!("Can not read directory {snapshot_path}"))?;
    let mut matches = Vec::new();

    while let Some(entry) = entries
        .next_entry()
        .await
        .context("Can not stat directory entry")?
    {
        let name = entry
            .path()
            .file_name()
            .and_then(|n| n.to_str())
            .map(String::from);
        if let Some(name) = name
            && name.starts_with("snapshot-")
            && name.contains("tar")
        {
            matches.push(name);
        };
    }
    Ok(matches)
}
