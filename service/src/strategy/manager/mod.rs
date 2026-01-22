use tokio::sync::mpsc;

use crate::strategy::PairedPosition;

pub struct PositionManager {
    pub positions: Vec<PairedPosition>,
    
    manager_tx: mpsc::Sender<PairedPosition>,
}

