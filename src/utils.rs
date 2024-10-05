use std::future::Future;
use tokio::time::{sleep, Duration};
use tracing::info;

pub async fn retry_with_backoff<T, E, Fut, F>(
    operation: F,
    initial_delay: Duration,
    max_retries: usize,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut retries = 0;
    let mut fib = (initial_delay, initial_delay);

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if retries < max_retries => {
                info!(
                    "Operation failed: {:?}. Retrying in {:?} (attempt {}/{})",
                    e,
                    fib.0,
                    retries + 1,
                    max_retries
                );
                sleep(fib.0).await;
                retries += 1;
                fib = (fib.1, fib.0 + fib.1);
            }
            Err(e) => return Err(e),
        }
    }
}
