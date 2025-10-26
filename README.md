# solana-snapshot-fetcher
Fetches snapshots for solana, fast and efficient.
Conceptual successor of [solana-snapshot-finder](github.com/Blockdaemon/solana-snapshot-finder) and its forks.

## What exactly does the program do:
1. Finds all available RPCs (via RPC)
2. Get the number of the current slot
3. In multi-threaded mode, checks the slot numbers of all snapshots on all RPCs
5. List of RPCs sorted by lowest latency
`slots_diff = current_slot - snapshot_slot`
5. Checks the download speed from RPC with the most recent snapshot. If `download_speed <min_download_speed`, then it checks the speed at the next node.
6. Downloads the snapshot

### Install
```bash
cargo build --release
```

Mainnet
```python
solana-snapshot-fetcher --snapshot_path $HOME/ledger
```

Testnet
```python
solana-snapshot-fetcher --snapshot_path $HOME/ledger -r http://api.testnet.solana.com
```

## Troubleshooting

```bash
RUST_LOG="solana_snapshot_fetcher=trace" cargo run
```

## Contributing

PRs are welcome. Let us not have a million forks of this tool.
Wishlist:
 * Support gossip spy to be able to fetch snapshots when no RPCs are running/available
 * Do not throw away the data fetched during speedtest
