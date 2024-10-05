use anyhow::Result;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

pub fn init_logging() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
