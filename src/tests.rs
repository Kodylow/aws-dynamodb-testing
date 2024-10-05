use crate::{
    dynamodb::{DynamoDb, FieldType, Item, Schema, Table},
    utils::retry_with_backoff,
};
use anyhow::Result;
use aws_sdk_dynamodb::types::AttributeValue;
use tokio::time::{sleep, Duration};
use tracing::{info, instrument};

const TEST_TABLE_NAME: &str = "test-products";
const CATEGORY_PARTITION_KEY: &str = "category";
const PRODUCT_NAME_SORT_KEY: &str = "product_name";
const PRICE_ATTRIBUTE: &str = "price";

#[instrument]
async fn setup_test_table(ddb: &DynamoDb) -> Result<Table<'static>> {
    let table = Table::new(
        TEST_TABLE_NAME,
        CATEGORY_PARTITION_KEY,
        Some(PRODUCT_NAME_SORT_KEY),
    )
    .with_schema(
        Schema::new()
            .add_field(CATEGORY_PARTITION_KEY, FieldType::String)
            .add_field(PRODUCT_NAME_SORT_KEY, FieldType::String)
            .add_field(PRICE_ATTRIBUTE, FieldType::Number),
    );

    if !ddb.table_exists(TEST_TABLE_NAME).await? {
        retry_with_backoff(
            || ddb.create_table_if_not_exists(&table),
            Duration::from_secs(3),
            5,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create table after multiple retries: {e:?}"))?
        .map(|_| {
            info!("Table created successfully");
        })
        .ok_or_else(|| anyhow::anyhow!("Table creation failed"))?;
    } else {
        info!("Table already exists");
    }

    Ok(table)
}

#[tokio::test]
#[instrument]
async fn test_dynamodb_operations() -> Result<()> {
    info!("Starting test_dynamodb_operations");
    dotenv::dotenv().ok();

    info!("Loading SDK config from env");
    let sdk_config = aws_config::load_from_env().await;
    info!("Creating DynamoDb instance");
    let ddb = DynamoDb::new(&sdk_config);

    info!("Setting up test table");
    let _table = setup_test_table(&ddb).await?;

    info!("Testing put_item");
    let item = Item::new()
        .set_string(CATEGORY_PARTITION_KEY, "Electronics")
        .set_string(PRODUCT_NAME_SORT_KEY, "Smartphone")
        .set_number(PRICE_ATTRIBUTE, 599.99);
    ddb.put_item(TEST_TABLE_NAME, item).await?;

    info!("Testing query_items");
    let partition_key = (
        CATEGORY_PARTITION_KEY,
        AttributeValue::S("Electronics".to_string()),
    );
    let items = ddb
        .query_items(TEST_TABLE_NAME, partition_key.clone(), None)
        .await?;
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get_string(PRODUCT_NAME_SORT_KEY),
        Some(&"Smartphone".to_string())
    );
    assert_eq!(items[0].get_number(PRICE_ATTRIBUTE), Some(599.99));

    info!("Testing update_item");
    let key = Item::new()
        .set_string(CATEGORY_PARTITION_KEY, "Electronics")
        .set_string(PRODUCT_NAME_SORT_KEY, "Smartphone");
    let updates = Item::new().set_number(PRICE_ATTRIBUTE, 649.99);
    ddb.update_item(TEST_TABLE_NAME, key, updates).await?;

    info!("Testing query_items after update");
    let items = ddb
        .query_items(TEST_TABLE_NAME, partition_key.clone(), None)
        .await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].get_number(PRICE_ATTRIBUTE), Some(649.99));

    info!("Testing delete_item");
    let key = Item::new()
        .set_string(CATEGORY_PARTITION_KEY, "Electronics")
        .set_string(PRODUCT_NAME_SORT_KEY, "Smartphone");
    ddb.delete_item(TEST_TABLE_NAME, key).await?;

    info!("Testing query_items after delete");
    let items = ddb
        .query_items(TEST_TABLE_NAME, partition_key, None)
        .await?;
    assert_eq!(items.len(), 0);

    info!("Cleaning up test data");
    let key = Item::new()
        .set_string(CATEGORY_PARTITION_KEY, "Electronics")
        .set_string(PRODUCT_NAME_SORT_KEY, "Smartphone");
    ddb.delete_item(TEST_TABLE_NAME, key).await?;

    info!("Test completed successfully");
    Ok(())
}

#[test]
fn test_item_operations() {
    let item = Item::new()
        .set_string("key1", "value1")
        .set_number("key2", 42.0);

    assert_eq!(item.get_string("key1"), Some(&"value1".to_string()));
    assert_eq!(item.get_number("key2"), Some(42.0));
    assert_eq!(item.get_string("non_existent"), None);
    assert_eq!(item.get_number("non_existent"), None);
}

#[test]
fn test_schema_operations() {
    let schema = Schema::new()
        .add_field("field1", FieldType::String)
        .add_field("field2", FieldType::Number);

    let fields = schema.fields();
    assert_eq!(fields.len(), 2);
    assert!(matches!(fields.get("field1"), Some(FieldType::String)));
    assert!(matches!(fields.get("field2"), Some(FieldType::Number)));
}

#[test]
fn test_table_operations() {
    let table = Table::new("test_table", "partition_key", Some("sort_key"));

    assert_eq!(table.name(), "test_table");
    assert_eq!(table.partition_key(), "partition_key");
    assert_eq!(table.sort_key(), Some("sort_key"));

    let schema = Schema::new()
        .add_field("field1", FieldType::String)
        .add_field("field2", FieldType::Number);
    let table_with_schema = table.with_schema(schema);

    assert!(table_with_schema.schema().is_some());
}
