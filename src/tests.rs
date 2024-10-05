//! Integration tests for DynamoDB operations
//!
//! These tests cover:
//! - Table creation and management
//! - Basic CRUD operations (Create, Read, Update, Delete)
//! - Querying with various conditions
//! - Authentication and table description
//! - Item, Schema, and Table struct operations
//! - Complex queries
//!
//! # Setup
//!
//! These tests require a running DynamoDB instance and proper AWS credentials.
//!
//! ## Credentials
//!
//! Set the following environment variables in your `.env` file:
//!
//! ```
//! AWS_ACCESS_KEY_ID=your_access_key
//! AWS_SECRET_ACCESS_KEY=your_secret_key
//! AWS_REGION=your_preferred_region
//! ```
//!
//! For local testing with DynamoDB Local, you can use dummy values and set:
//!
//! ```
//! AWS_ENDPOINT_URL=http://localhost:8000
//! ```
//!
//! ## Test Table
//!
//! The tests use a table named "test-products" with the following structure:
//! - Partition key: "category" (String)
//! - Sort key: "product_name" (String)
//! - Additional attribute: "price" (Number)
//!
//! The table is created at the start of relevant tests if it doesn't exist.
//!
//! # Running Tests
//!
//! To run these tests, use:
//!
//! ```
//! cargo test --test integration
//! ```
//!
//! Note: These tests may incur AWS charges if run against a real DynamoDB instance.

use crate::{
    constants::{CATEGORY_PARTITION_KEY, PRICE_ATTRIBUTE, PRODUCT_NAME_SORT_KEY},
    dynamodb::{DynamoDb, FieldType, Item, Schema, Table},
};
use anyhow::Result;
use aws_sdk_dynamodb::types::AttributeValue;
use dotenv::dotenv;
use std::collections::HashMap;
use std::time::Instant;
use tokio::time::Duration;
use tracing::{error, info, instrument};

const TEST_TABLE_NAME: &str = "testing-products";

#[instrument]
async fn setup_test_table(ddb: &DynamoDb) -> Result<Table<'static>> {
    let start = Instant::now();
    info!("Setting up test table: {}", TEST_TABLE_NAME);

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
        match crate::utils::retry_with_backoff(
            || ddb.create_table_if_not_exists(&table),
            Duration::from_secs(3),
            5,
        )
        .await
        {
            Ok(Some(_)) => {
                info!("Table created successfully in {:?}", start.elapsed());
            }
            Ok(None) => {
                info!("Table already exists");
            }
            Err(e) => {
                error!("Failed to create table after multiple retries: {e:?}");
                return Err(anyhow::anyhow!(
                    "Failed to create table after multiple retries: {e:?}"
                ));
            }
        }
    } else {
        info!("Table already exists");
    }

    // Wait for the table to become active
    let mut attempts = 0;
    while attempts < 10 {
        match ddb.describe_table(TEST_TABLE_NAME).await {
            Ok(description) => {
                if let Some(table_description) = description.table() {
                    if table_description.table_status()
                        == Some(&aws_sdk_dynamodb::types::TableStatus::Active)
                    {
                        info!("Table is active");
                        break;
                    }
                }
            }
            Err(e) => {
                error!("Error describing table: {e:?}");
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        attempts += 1;
    }

    if attempts == 10 {
        return Err(anyhow::anyhow!(
            "Table did not become active within the expected time"
        ));
    }

    info!("Test table setup completed in {:?}", start.elapsed());
    Ok(table)
}

async fn clean_up_testing_table(ddb: &DynamoDb) -> Result<()> {
    let items = ddb.scan_table(TEST_TABLE_NAME).await?;
    for item in items {
        let key = Item::new()
            .set_string(
                CATEGORY_PARTITION_KEY,
                item.get(CATEGORY_PARTITION_KEY)
                    .and_then(|attr| attr.as_s().ok())
                    .ok_or_else(|| anyhow::anyhow!("Missing or invalid partition key"))?,
            )
            .set_string(
                PRODUCT_NAME_SORT_KEY,
                item.get(PRODUCT_NAME_SORT_KEY)
                    .and_then(|attr| attr.as_s().ok())
                    .ok_or_else(|| anyhow::anyhow!("Missing or invalid sort key"))?,
            );
        ddb.delete_item(TEST_TABLE_NAME, key).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use aws_config::load_from_env;

    async fn setup() -> Result<DynamoDb> {
        dotenv().ok();
        let sdk_config = load_from_env().await;
        Ok(DynamoDb::new(&sdk_config))
    }

    async fn run_test<F, Fut>(test_name: &str, test_fn: F) -> Result<()>
    where
        F: FnOnce(DynamoDb) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let start = Instant::now();
        info!("Starting test: {}", test_name);

        let ddb = setup().await.context("Failed to setup DynamoDB client")?;
        let _table = setup_test_table(&ddb)
            .await
            .context("Failed to setup test table")?;

        // Add a delay to ensure the table is fully created
        tokio::time::sleep(Duration::from_secs(5)).await;

        let result = test_fn(ddb).await;

        match &result {
            Ok(_) => info!("Test '{}' passed in {:?}", test_name, start.elapsed()),
            Err(e) => error!("Test '{}' failed: {:?}", test_name, e),
        }

        result
    }

    #[tokio::test]
    async fn test_table_creation_and_deletion() -> Result<()> {
        run_test("table_creation_and_deletion", |ddb| async move {
            assert!(ddb.table_exists(TEST_TABLE_NAME).await?);

            // Add a delay to ensure the table is fully created
            tokio::time::sleep(Duration::from_secs(5)).await;

            ddb.delete_table(TEST_TABLE_NAME).await?;

            // Add a delay to ensure the table is fully deleted
            tokio::time::sleep(Duration::from_secs(5)).await;

            assert!(!ddb.table_exists(TEST_TABLE_NAME).await?);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_basic_crud_operations() -> Result<()> {
        run_test("basic_crud_operations", |ddb| async move {
            // Test put_item
            let item = Item::new()
                .set_string(CATEGORY_PARTITION_KEY, "Electronics")
                .set_string(PRODUCT_NAME_SORT_KEY, "Smartphone")
                .set_number(PRICE_ATTRIBUTE, 599.99);
            ddb.put_item(TEST_TABLE_NAME, item)
                .await
                .context("Failed to put item")?;

            // Test get_item
            let key = Item::new()
                .set_string(CATEGORY_PARTITION_KEY, "Electronics")
                .set_string(PRODUCT_NAME_SORT_KEY, "Smartphone");
            let retrieved_item = ddb.get_item(TEST_TABLE_NAME, key.clone()).await?;
            let retrieved_item = retrieved_item.ok_or_else(|| anyhow::anyhow!("Item not found"))?;
            assert_eq!(
                retrieved_item.get_number(PRICE_ATTRIBUTE),
                Some(599.99),
                "Unexpected price value"
            );

            // Test update_item
            let updates = Item::new().set_number(PRICE_ATTRIBUTE, 649.99);
            ddb.update_item(TEST_TABLE_NAME, key.clone(), updates)
                .await
                .context("Failed to update item")?;
            let updated_item = ddb.get_item(TEST_TABLE_NAME, key.clone()).await?;
            let updated_item =
                updated_item.ok_or_else(|| anyhow::anyhow!("Updated item not found"))?;
            assert_eq!(
                updated_item.get_number(PRICE_ATTRIBUTE),
                Some(649.99),
                "Unexpected updated price value"
            );

            // Test delete_item
            ddb.delete_item(TEST_TABLE_NAME, key.clone()).await?;
            let deleted_item = ddb.get_item(TEST_TABLE_NAME, key.clone()).await?;
            assert!(deleted_item.is_none(), "Item was not deleted");

            clean_up_testing_table(&ddb)
                .await
                .context("Failed to clean up testing table")?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_query_operations() -> Result<()> {
        run_test("query_operations", |ddb| async move {
            // Ensure table is created and wait for it to be active
            let _table = setup_test_table(&ddb).await?;

            // Add test items
            for i in 1..=5 {
                let item = Item::new()
                    .set_string(CATEGORY_PARTITION_KEY, "Electronics")
                    .set_string(PRODUCT_NAME_SORT_KEY, format!("Product{}", i))
                    .set_number(PRICE_ATTRIBUTE, (i as f64) * 100.0);
                ddb.put_item(TEST_TABLE_NAME, item).await?;
            }

            // Add a delay to ensure items are fully added
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Test query_items
            let partition_key = (
                CATEGORY_PARTITION_KEY,
                AttributeValue::S("Electronics".to_string()),
            );
            let items = ddb
                .query_simple(
                    TEST_TABLE_NAME,
                    partition_key.clone(),
                    None,
                    None,
                    None,
                    None,
                )
                .await?;
            assert_eq!(items.len(), 5);

            // Test query_simple
            let sort_key_condition = Some((
                PRODUCT_NAME_SORT_KEY,
                ">".to_string(),
                AttributeValue::S("Product2".to_string()),
            ));
            let filter_expression = Some("price > :min_price");
            let mut expression_attribute_values = HashMap::new();
            expression_attribute_values.insert(
                ":min_price".to_string(),
                AttributeValue::N("200".to_string()),
            );
            let queried_items = ddb
                .query_simple(
                    TEST_TABLE_NAME,
                    partition_key,
                    sort_key_condition,
                    filter_expression,
                    Some(3),
                    Some(expression_attribute_values),
                )
                .await?;
            assert_eq!(queried_items.len(), 3);

            clean_up_testing_table(&ddb)
                .await
                .context("Failed to clean up testing table")?;
            Ok(())
        })
        .await
    }

    #[test]
    fn test_item_schema_and_table_operations() {
        // Test Item operations
        let item = Item::new()
            .set_string("key1", "value1")
            .set_number("key2", 42.0);
        assert_eq!(item.get_string("key1"), Some(&"value1".to_string()));
        assert_eq!(item.get_number("key2"), Some(42.0));

        // Test Schema operations
        let schema = Schema::new()
            .add_field("field1", FieldType::String)
            .add_field("field2", FieldType::Number);
        let fields = schema.fields();
        assert_eq!(fields.len(), 2);
        assert!(matches!(fields.get("field1"), Some(FieldType::String)));
        assert!(matches!(fields.get("field2"), Some(FieldType::Number)));

        // Test Table operations
        let table = Table::new("test_table", "partition_key", Some("sort_key"));
        assert_eq!(table.name(), "test_table");
        assert_eq!(table.partition_key(), "partition_key");
        assert_eq!(table.sort_key(), Some("sort_key"));
    }

    #[tokio::test]
    async fn test_auth_and_describe_table() -> Result<()> {
        run_test("auth_and_describe_table", |ddb| async move {
            // Test check_auth
            ddb.check_auth().await?;

            // Test describe_table
            let description = ddb.describe_table(TEST_TABLE_NAME).await?;
            let table = description
                .table()
                .ok_or_else(|| anyhow::anyhow!("Table description not found"))?;
            assert_eq!(
                table.table_name(),
                Some(TEST_TABLE_NAME),
                "Unexpected table name"
            );

            clean_up_testing_table(&ddb)
                .await
                .context("Failed to clean up testing table")?;
            Ok(())
        })
        .await
    }
}
