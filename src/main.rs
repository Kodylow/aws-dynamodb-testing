mod dynamodb;
mod logging;

use anyhow::Result;
use tracing::info;

pub const TABLE_NAME: &str = "testing-products";

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging()?;
    dotenv::dotenv().ok();

    let sdk_config = aws_config::load_from_env().await;
    let ddb_client = aws_sdk_dynamodb::Client::new(&sdk_config);

    dynamodb::check_authentication(&ddb_client).await?;

    let table_result = dynamodb::create_table_if_not_exists(&ddb_client, TABLE_NAME).await;
    match table_result {
        Ok(table) => {
            if let Some(description) = table.table_description() {
                info!("Table status: {:?}", description.table_status());
                info!("Table name: {:?}", description.table_name());
            } else {
                info!("Table created, but description is not available");
            }
        }
        Err(e) => info!("Error creating table: {}", e),
    }

    Ok(())
}
