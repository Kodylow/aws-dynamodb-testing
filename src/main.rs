mod dynamodb;
mod logging;

use std::collections::HashMap;

use anyhow::Result;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::info;

const TABLE_NAME: &str = "testing-products";
const CATEGORY_PARTITION_KEY: &str = "category";
const PRODUCT_NAME_SORT_KEY: &str = "product_name";
const PRICE_ATTRIBUTE: &str = "price";

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging()?;
    dotenv::dotenv().ok();

    let sdk_config = aws_config::load_from_env().await;

    let ddb = dynamodb::DynamoDbApp::new(&sdk_config);

    ddb.check_auth().await?;

    ddb.create_table_if_not_exists(
        TABLE_NAME,
        CATEGORY_PARTITION_KEY,
        Some(PRODUCT_NAME_SORT_KEY),
    )
    .await?;

    let put_item_result = ddb
        .put_item(
            TABLE_NAME,
            HashMap::from([
                (
                    CATEGORY_PARTITION_KEY.to_string(),
                    AttributeValue::S("living-room".to_string()),
                ),
                (
                    PRODUCT_NAME_SORT_KEY.to_string(),
                    AttributeValue::S("couch".to_string()),
                ),
                (
                    PRICE_ATTRIBUTE.to_string(),
                    AttributeValue::N("375.0".to_string()),
                ),
            ]),
        )
        .await?;

    info!("Put item result: {:?}", put_item_result);

    Ok(())
}
