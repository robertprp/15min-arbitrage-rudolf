# Rudolf

Rudolf is an experimental Rust arbitrage bot for Polymarket and Kalshi. It monitors matching 15-minute Bitcoin prediction markets, compares the cost of complementary YES/NO contracts across both exchanges, and attempts paired trades when the price difference exceeds a configured threshold.

It combines WebSocket order books, Chainlink and Kalshi reference prices, position tracking, entry and exit risk checks, and automatic rollover between market windows.

## Project layout

- `src/`: command-line entry point, market monitoring, and background task cleanup.
- `service/`: exchange clients, configuration, price feeds, order books, strategy, and position management.
- `lib/`: shared result and error types.

## Setup

Use a Rust toolchain with edition 2024 support. The workspace also uses native TLS dependencies, which may require platform development tools and OpenSSL on Linux.

```sh
cp .env.example .env
cargo build --locked --release
```

Fill in every variable in `.env` with your own credentials. `KALSHI_PRIVATE_KEY_PEM` is a path to a local PEM file, rather than the key contents. Polymarket order authentication currently uses a Gnosis Safe funder on Polygon. Configuration loads from the process environment and the default `.env` file.

## Usage

Inspect the command without connecting to the exchanges:

```sh
cargo run --locked -- --help
cargo run --locked -- arbitrage --help
```

With credentials configured, the following command starts live trading:

```sh
cargo run --locked --release -- arbitrage --symbol btc --min-edge 0.02
```

`--min-edge` is currently compared directly with contract prices in dollars: `0.02` represents two cents per paired contract. The monitor currently selects BTC internally, even though the CLI also accepts ETH. The `--config` argument is declared but is not yet connected to configuration loading; use `.env` or environment variables.

## Development status

This is a work in progress with live order execution and no paper-trading mode. Trade sizing, position limits, cooldowns, and exit thresholds are currently defined in the strategy source. Error recovery for a trade that fills on only one exchange is incomplete. Review those paths and the exchanges' current market definitions before running it with funded accounts.

Build and inspect the workspace without starting the bot:

```sh
cargo check --locked --workspace
cargo test --locked --workspace
cargo fmt --all -- --check
```
