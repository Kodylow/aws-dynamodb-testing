//! Initializes application logging using `tracing` and `tracing_subscriber`.

use anyhow::Result;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

/// Sets up global logging with INFO level, line numbers, and file names.
///
/// # Errors
///
/// Returns an error if setting the global default subscriber fails.
pub fn init() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
