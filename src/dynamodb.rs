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
#[derive(Debug)]
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

    /// Returns the name of the table.
    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the partition key of the table.
    pub fn partition_key(&self) -> &str {
        self.partition_key
    }

    /// Returns the sort key of the table, if any.
    pub fn sort_key(&self) -> Option<&str> {
        self.sort_key
    }

    /// Sets the schema for the table and returns the modified `Table`.
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Returns a reference to the table's schema, if set.
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
    ///
    /// Returns `Some(CreateTableOutput)` if a new table was created, or `None` if the table already exists.
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

    /// Deletes a table if it exists.
    pub async fn delete_table(&self, table_name: &str) -> Result<()> {
        self.client
            .delete_table()
            .table_name(table_name)
            .send()
            .await?;
        info!("Table '{table_name}' deleted");
        Ok(())
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
    pub async fn table_exists(&self, table_name: &str) -> Result<bool> {
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
    ///
    /// Returns a vector of items, where each item is represented as a HashMap of attribute name-value pairs.
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

    /// Gets an item from a DynamoDB table.
    ///
    /// Returns `Some(Item)` if the item is found, or `None` if it doesn't exist.
    pub async fn get_item(&self, table_name: &str, key: Item) -> Result<Option<Item>> {
        let response = self
            .client
            .get_item()
            .table_name(table_name)
            .set_key(Some(key.attributes))
            .send()
            .await?;

        Ok(response.item.map(|attrs| Item { attributes: attrs }))
    }

    /// Updates an item in a DynamoDB table.
    pub async fn update_item(&self, table_name: &str, key: Item, updates: Item) -> Result<()> {
        let mut update_expression = String::new();
        let mut expression_attribute_names = HashMap::new();
        let mut expression_attribute_values = HashMap::new();

        for (i, (attr_name, attr_value)) in updates.attributes.iter().enumerate() {
            let placeholder = format!("#attr{}", i);
            let value_placeholder = format!(":val{}", i);

            if i > 0 {
                update_expression.push_str(", ");
            }
            update_expression.push_str(&format!("{} = {}", placeholder, value_placeholder));

            expression_attribute_names.insert(placeholder, attr_name.clone());
            expression_attribute_values.insert(value_placeholder, attr_value.clone());
        }

        self.client
            .update_item()
            .table_name(table_name)
            .set_key(Some(key.attributes))
            .update_expression(format!("SET {}", update_expression))
            .set_expression_attribute_names(Some(expression_attribute_names))
            .set_expression_attribute_values(Some(expression_attribute_values))
            .send()
            .await?;

        info!("Item updated in '{table_name}'");
        Ok(())
    }

    /// Deletes an item from a DynamoDB table.
    pub async fn delete_item(&self, table_name: &str, key: Item) -> Result<()> {
        self.client
            .delete_item()
            .table_name(table_name)
            .set_key(Some(key.attributes))
            .send()
            .await?;

        info!("Item deleted from '{table_name}'");
        Ok(())
    }

    /// Queries items from a DynamoDB table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to query.
    /// * `partition_key` - A tuple containing the partition key name and value.
    /// * `sort_key_condition` - An optional tuple containing the sort key name, condition, and value.
    ///
    /// # Returns
    ///
    /// A vector of `Item`s matching the query conditions.
    pub async fn query_items(
        &self,
        table_name: &str,
        partition_key: (&str, AttributeValue),
        sort_key_condition: Option<(&str, String, AttributeValue)>,
    ) -> Result<Vec<Item>> {
        let mut query = self
            .client
            .query()
            .table_name(table_name)
            .key_condition_expression("#pk = :pkval")
            .expression_attribute_names("#pk", partition_key.0)
            .expression_attribute_values(":pkval", partition_key.1);

        if let Some((sort_key, condition, value)) = sort_key_condition {
            query = query
                .key_condition_expression(format!(
                    "#pk = :pkval AND {} {} :skval",
                    sort_key, condition
                ))
                .expression_attribute_names("#sk", sort_key)
                .expression_attribute_values(":skval", value);
        }

        let response = query.send().await?;

        Ok(response
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|attrs| Item { attributes: attrs })
            .collect())
    }
}

/// Represents a DynamoDB item with various attribute types.
#[derive(Default, Debug)]
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

    /// Gets the value of an attribute as a string.
    pub fn get_string(&self, key: &str) -> Option<&String> {
        self.attributes.get(key).and_then(|av| av.as_s().ok())
    }

    /// Gets the value of an attribute as a number (f64).
    pub fn get_number(&self, key: &str) -> Option<f64> {
        self.attributes
            .get(key)
            .and_then(|av| av.as_n().ok())
            .and_then(|n| n.parse().ok())
    }
}

/// Represents the schema of a DynamoDB table.
#[derive(Debug, Clone)]
pub struct Schema {
    fields: HashMap<String, FieldType>,
}

/// Represents the type of a field in a DynamoDB table schema.
#[derive(Debug, Clone)]
pub enum FieldType {
    /// Represents a string field.
    String,
    /// Represents a number field.
    Number,
    // Add more types as needed
}

impl Schema {
    /// Creates a new empty `Schema`.
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Adds a field to the schema and returns the modified `Schema`.
    pub fn add_field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.fields.insert(name.into(), field_type);
        self
    }

    /// Returns a reference to the fields in the schema.
    pub fn fields(&self) -> &HashMap<String, FieldType> {
        &self.fields
    }
}
