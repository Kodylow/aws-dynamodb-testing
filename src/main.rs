mod command_line;
mod dynamodb;
mod logging;
use anyhow::Result;
use dynamodb::{FieldType, Schema, Table};

const TABLE_NAME: &str = "testing-products";
const CATEGORY_PARTITION_KEY: &str = "category";
const PRODUCT_NAME_SORT_KEY: &str = "product_name";
const PRICE_ATTRIBUTE: &str = "price";

#[tokio::main]
async fn main() -> Result<()> {
    logging::init()?;
    dotenv::dotenv().ok();

    let sdk_config = aws_config::load_from_env().await;
    let ddb = dynamodb::DynamoDb::new(&sdk_config);

    ddb.check_auth().await?;

    let schema = Schema::new()
        .add_field(CATEGORY_PARTITION_KEY, FieldType::String)
        .add_field(PRODUCT_NAME_SORT_KEY, FieldType::String)
        .add_field(PRICE_ATTRIBUTE, FieldType::Number);

    let table = Table::new(
        TABLE_NAME,
        CATEGORY_PARTITION_KEY,
        Some(PRODUCT_NAME_SORT_KEY),
    )
    .with_schema(schema);

    ddb.create_table_if_not_exists(&table).await?;

    command_line::run(&ddb, &table).await?;

    Ok(())
}
