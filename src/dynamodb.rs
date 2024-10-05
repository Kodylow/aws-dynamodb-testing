//! High-level interface for Amazon DynamoDB operations.
//!
//! Provides structures and methods for table management and item manipulation.

use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::{
    operation::{create_table::CreateTableOutput, scan::ScanOutput},
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
    Client,
};
use std::collections::HashMap;
use tracing::{error, info};

/// DynamoDB client wrapper for high-level operations.
pub struct DynamoDb {
    client: Client,
}

/// DynamoDB table configuration.
#[derive(Debug)]
pub struct Table<'a> {
    name: &'a str,
    partition_key: &'a str,
    sort_key: Option<&'a str>,
    schema: Option<Schema>,
}

impl<'a> Table<'a> {
    /// Creates a new `Table` instance.
    pub fn new(name: &'a str, partition_key: &'a str, sort_key: Option<&'a str>) -> Self {
        Self {
            name,
            partition_key,
            sort_key,
            schema: None,
        }
    }

    pub fn name(&self) -> &str {
        self.name
    }

    pub fn partition_key(&self) -> &str {
        self.partition_key
    }

    pub fn sort_key(&self) -> Option<&str> {
        self.sort_key
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }
}

impl DynamoDb {
    /// Creates a new `DynamoDb` instance.
    pub fn new(sdk_config: &aws_config::SdkConfig) -> Self {
        Self {
            client: Client::new(sdk_config),
        }
    }

    /// Verifies authentication by attempting to list tables.
    pub async fn check_auth(&self) -> Result<()> {
        self.client.list_tables().send().await.map_err(|e| {
            error!("Authentication failed: {}", e);
            anyhow!("Authentication failed")
        })?;
        info!("Authentication successful");
        Ok(())
    }

    /// Creates a table if it doesn't exist.
    pub async fn create_table_if_not_exists(
        &self,
        table: &Table<'_>,
    ) -> Result<Option<CreateTableOutput>> {
        if self.table_exists(table.name).await? {
            info!("Table '{}' exists", table.name);
            return Ok(None);
        }

        let mut attribute_definitions = vec![AttributeDefinition::builder()
            .attribute_name(table.partition_key)
            .attribute_type(ScalarAttributeType::S)
            .build()?];

        let mut key_schema = vec![KeySchemaElement::builder()
            .attribute_name(table.partition_key)
            .key_type(KeyType::Hash)
            .build()?];

        if let Some(sort_key) = table.sort_key {
            attribute_definitions.push(
                AttributeDefinition::builder()
                    .attribute_name(sort_key)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            );
            key_schema.push(
                KeySchemaElement::builder()
                    .attribute_name(sort_key)
                    .key_type(KeyType::Range)
                    .build()?,
            );
        }

        let output = self
            .client
            .create_table()
            .table_name(table.name)
            .billing_mode(BillingMode::PayPerRequest)
            .set_attribute_definitions(Some(attribute_definitions))
            .set_key_schema(Some(key_schema))
            .send()
            .await?;
        Ok(Some(output))
    }

    /// Puts an item into a DynamoDB table.
    pub async fn put_item(&self, table_name: &str, item: Item) -> Result<()> {
        self.client
            .put_item()
            .table_name(table_name)
            .set_item(Some(item.attributes))
            .send()
            .await?;

        info!("Item added to '{table_name}'");
        Ok(())
    }

    /// Checks if a table exists.
    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let tables = self.client.list_tables().send().await?;
        Ok(tables.table_names().contains(&table_name.to_string()))
    }

    /// Retrieves table description.
    pub async fn describe_table(
        &self,
        table_name: &str,
    ) -> Result<aws_sdk_dynamodb::operation::describe_table::DescribeTableOutput> {
        self.client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(Into::into)
    }

    /// Scans a table for items.
    pub async fn scan_table(
        &self,
        table_name: &str,
    ) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let mut items = Vec::new();
        let mut last_evaluated_key = None;

        loop {
            let mut scan = self.client.scan().table_name(table_name);

            if let Some(key) = last_evaluated_key {
                scan = scan.set_exclusive_start_key(Some(key));
            }

            let response: ScanOutput = scan.send().await?;

            if let Some(new_items) = response.items {
                items.extend(new_items);
            }

            last_evaluated_key = response.last_evaluated_key;

            if last_evaluated_key.is_none() {
                break;
            }
        }

        Ok(items)
    }
}

/// Represents a DynamoDB item with various attribute types.
#[derive(Default)]
pub struct Item {
    attributes: HashMap<String, AttributeValue>,
}

impl Item {
    /// Creates a new empty `Item`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a string attribute.
    pub fn set_string(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes
            .insert(key.into(), AttributeValue::S(value.into()));
        self
    }

    /// Sets a number attribute.
    pub fn set_number(mut self, key: impl Into<String>, value: impl Into<f64>) -> Self {
        self.attributes
            .insert(key.into(), AttributeValue::N(value.into().to_string()));
        self
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    fields: HashMap<String, FieldType>,
}

#[derive(Debug, Clone)]
pub enum FieldType {
    String,
    Number,
    // Add more types as needed
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn add_field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.fields.insert(name.into(), field_type);
        self
    }

    pub fn fields(&self) -> &HashMap<String, FieldType> {
        &self.fields
    }
}
