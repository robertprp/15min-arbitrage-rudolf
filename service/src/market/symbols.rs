use clap::ValueEnum;

#[derive(Debug, Clone, ValueEnum, PartialEq)]
pub enum Min15Symbols {
    BTC,
    ETH,
    // SOL,
    // XRP,
}

#[derive(Debug, Clone, ValueEnum, PartialEq)]
pub enum Hour1Symbols {
    BTC,
    ETH,
}
