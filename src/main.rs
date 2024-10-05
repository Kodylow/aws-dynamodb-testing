mod command_line;
mod constants;
mod dynamodb;
mod logging;
#[cfg(test)]
mod tests;
mod utils;

use anyhow::Result;
use constants::{CATEGORY_PARTITION_KEY, PRICE_ATTRIBUTE, PRODUCT_NAME_SORT_KEY, TABLE_NAME};
use dynamodb::{FieldType, Schema, Table};

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
