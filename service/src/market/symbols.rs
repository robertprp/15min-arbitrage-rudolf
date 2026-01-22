use clap::ValueEnum;

#[derive(Debug, Clone, ValueEnum)]
pub enum Min15Symbols {
    BTC,
    ETH,
    // SOL,
    // XRP,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum Hour1Symbols {
    BTC,
    ETH,
}
