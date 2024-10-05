use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::{
    operation::create_table::CreateTableOutput,
    types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType},
    Client,
};
use tracing::{error, info};

pub const CATEGORY_PARTITION_KEY: &str = "category";
pub const PRODUCT_NAME_SORT_KEY: &str = "product_name";

pub async fn check_authentication(client: &Client) -> Result<()> {
    match client.list_tables().send().await {
        Ok(_) => {
            info!("Authentication successful. Credentials are valid.");
            Ok(())
        }
        Err(e) => {
            error!("Authentication failed. Error: {}", e);
            Err(anyhow!("Authentication failed"))
        }
    }
}

pub async fn create_table_if_not_exists(
    client: &Client,
    table_name: &str,
) -> Result<CreateTableOutput> {
    if table_exists(client, table_name).await? {
        info!("Table '{}' already exists", table_name);
        return Err(anyhow!("Table already exists"));
    }

    let attr_part = AttributeDefinition::builder()
        .attribute_name(CATEGORY_PARTITION_KEY)
        .attribute_type(ScalarAttributeType::S)
        .build()?;

    let attr_sort = AttributeDefinition::builder()
        .attribute_name(PRODUCT_NAME_SORT_KEY)
        .attribute_type(ScalarAttributeType::S)
        .build()?;

    let key_schema_part = KeySchemaElement::builder()
        .attribute_name(CATEGORY_PARTITION_KEY)
        .key_type(KeyType::Hash)
        .build()?;

    let key_schema_sort = KeySchemaElement::builder()
        .attribute_name(PRODUCT_NAME_SORT_KEY)
        .key_type(KeyType::Range)
        .build()?;

    let create_table_output = client
        .create_table()
        .table_name(table_name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(attr_part)
        .attribute_definitions(attr_sort)
        .key_schema(key_schema_part)
        .key_schema(key_schema_sort)
        .send()
        .await?;

    Ok(create_table_output)
}

async fn table_exists(client: &Client, table_name: &str) -> Result<bool> {
    let tables = client.list_tables().send().await?;
    Ok(tables.table_names().contains(&table_name.to_string()))
}
