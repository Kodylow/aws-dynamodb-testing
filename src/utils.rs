use std::future::Future;
use tokio::time::{sleep, Duration};
use tracing::info;

/// Retries an asynchronous operation with exponential backoff.
///
/// This function will attempt to execute the provided operation, retrying with
/// increasing delays between attempts if it fails. The delay between retries
/// follows a Fibonacci sequence, starting from the initial delay.
///
/// # Arguments
///
/// * `operation` - A closure that returns a `Future` representing the operation to be retried.
/// * `initial_delay` - The initial delay duration before the first retry.
/// * `max_retries` - The maximum number of retry attempts before giving up.
///
/// # Type Parameters
///
/// * `T` - The success type of the operation.
/// * `E` - The error type of the operation, which must implement `std::fmt::Debug`.
/// * `Fut` - The future type returned by the operation.
/// * `F` - The type of the closure that returns the operation future.
///
/// # Returns
///
/// Returns a `Result<T, E>` which is either the successful result of the operation,
/// or the last error encountered if all retry attempts fail.
///
/// # Examples
///
/// ```
/// use tokio::time::Duration;
/// use your_crate::utils::retry_with_backoff;
///
/// async fn fallible_operation() -> Result<(), std::io::Error> {
///     // Your operation logic here
///     Ok(())
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), std::io::Error> {
///     let result = retry_with_backoff(
///         || fallible_operation(),
///         Duration::from_secs(1),
///         3
///     ).await?;
///     Ok(())
/// }
/// ```
#[allow(dead_code)]
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
