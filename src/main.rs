mod dynamodb;
mod logging;

use anyhow::Result;
use tracing::info;

const TABLE_NAME: &str = "testing-products";
const PARTITION_KEY: &str = "category";
const SORT_KEY: &str = "product_name";

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging()?;
    dotenv::dotenv().ok();

    let sdk_config = aws_config::load_from_env().await;

    let ddb = dynamodb::DynamoDbApp::new(&sdk_config);

    ddb.check_authentication().await?;

    match ddb
        .create_table_if_not_exists(TABLE_NAME, PARTITION_KEY, Some(SORT_KEY))
        .await
    {
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
