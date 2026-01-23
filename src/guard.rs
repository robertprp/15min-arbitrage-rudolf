use tokio::task::JoinHandle;
use tracing::info;

/// A helper struct to ensure tasks are aborted when the window closes/drops.
pub struct TaskGuard {
    pub handles: Vec<JoinHandle<()>>,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if !self.handles.is_empty() {
            info!(
                "🧹 Cleaning up {} background tasks from previous window...",
                self.handles.len()
            );
            for handle in &self.handles {
                handle.abort();
            }
        }
    }
}
