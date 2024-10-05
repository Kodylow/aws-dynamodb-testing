mod dynamodb;
mod logging;
use anyhow::Result;
use dynamodb::Item;
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

    let ddb = dynamodb::DynamoDb::new(&sdk_config);

    ddb.check_auth().await?;

    ddb.create_table_if_not_exists(&dynamodb::Table::new(
        TABLE_NAME,
        CATEGORY_PARTITION_KEY,
        Some(PRODUCT_NAME_SORT_KEY),
    ))
    .await?;

    let put_item_result = ddb
        .put_item(
            TABLE_NAME,
            Item::new()
                .set_string(CATEGORY_PARTITION_KEY, "living-room")
                .set_string(PRODUCT_NAME_SORT_KEY, "couch")
                .set_number(PRICE_ATTRIBUTE, "375.0"),
        )
        .await?;

    info!("Put item result: {:?}", put_item_result);

    Ok(())
}
