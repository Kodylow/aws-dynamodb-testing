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

use crate::dynamodb::{Item, Table};

/// DynamoDB client wrapper for high-level operations.
///
/// This struct provides a convenient interface for interacting with Amazon DynamoDB,
/// abstracting away many of the low-level details of the AWS SDK.
///
/// # Features
///
/// - Table management: Create, delete, and check existence of tables
/// - Item operations: Put, get, update, and delete items
/// - Querying and scanning: Flexible querying and scanning of tables
/// - Authentication: Verify AWS credentials
///
/// # DynamoDB Concepts
///
/// ## Tables
/// In DynamoDB, a table is a collection of items (rows), and each item consists of attributes (columns).
/// Tables are schema-less, allowing each item to have a different structure.
///
/// ## Primary Key
/// Each table must have a primary key, which can be:
/// - Partition Key: A single attribute that DynamoDB uses to distribute data across partitions
/// - Composite Key: A combination of Partition Key and Sort Key
///
/// ## Operations
/// - **Put**: Add a new item to a table
/// - **Get**: Retrieve an item by its primary key
/// - **Update**: Modify an existing item's attributes
/// - **Delete**: Remove an item from a table
/// - **Query**: Retrieve items based on primary key values
/// - **Scan**: Read every item in a table
///
/// # Example
///
/// ```rust
/// use aws_config::load_from_env;
/// use dynamodb::{DynamoDb, Table, Item};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = load_from_env().await;
///     let client = DynamoDb::new(&config);
///
///     // Create a table
///     let table = Table::new("users", "user_id", Some("email"));
///     client.create_table_if_not_exists(&table).await?;
///
///     // Put an item
///     let item = Item::new()
///         .set("user_id", "123")
///         .set("email", "user@example.com")
///         .set("name", "John Doe");
///     client.put_item("users", item).await?;
///
///     // Query items
///     let items = client.query_items(
///         "users",
///         ("user_id", AttributeValue::S("123".to_string())),
///         None
///     ).await?;
///
///     Ok(())
/// }
/// ```
///
/// # Performance Considerations
///
/// - Use `query` instead of `scan` when possible for better performance
/// - Consider using batch operations for multiple items
/// - Be mindful of provisioned throughput limits
///
/// # Error Handling
///
/// Most methods return `Result<T, anyhow::Error>`, allowing for flexible error handling.
/// Use the `?` operator or match on the `Result` to handle potential errors.
#[derive(Debug)]
pub struct DynamoDb {
    client: Client,
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

    // --- Table Operations ---

    /// Creates a table if it doesn't exist.
    pub async fn create_table_if_not_exists(
        &self,
        table: &Table<'_>,
    ) -> Result<Option<CreateTableOutput>> {
        if self.table_exists(table.name()).await? {
            info!("Table '{}' exists", table.name());
            return Ok(None);
        }

        let mut attribute_definitions = vec![AttributeDefinition::builder()
            .attribute_name(table.partition_key())
            .attribute_type(ScalarAttributeType::S)
            .build()?];

        let mut key_schema = vec![KeySchemaElement::builder()
            .attribute_name(table.partition_key())
            .key_type(KeyType::Hash)
            .build()?];

        if let Some(sort_key) = table.sort_key() {
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
            .table_name(table.name())
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

    // --- Item Operations ---

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

    /// Gets an item from a DynamoDB table.
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

    // --- Query and Scan Operations ---

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

    /// Performs a scan operation on a DynamoDB table.
    pub async fn scan(
        &self,
        table_name: &str,
        filter_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Result<Vec<Item>> {
        let mut items = Vec::new();
        let mut last_evaluated_key = None;

        loop {
            let mut scan = self.client.scan().table_name(table_name);

            if let Some(filter) = &filter_expression {
                scan = scan.filter_expression(filter);
            }

            if let Some(names) = &expression_attribute_names {
                scan = scan.set_expression_attribute_names(Some(names.clone()));
            }

            if let Some(values) = &expression_attribute_values {
                scan = scan.set_expression_attribute_values(Some(values.clone()));
            }

            if let Some(key) = last_evaluated_key {
                scan = scan.set_exclusive_start_key(Some(key));
            }

            let response = scan.send().await?;

            if let Some(new_items) = response.items {
                items.extend(
                    new_items
                        .into_iter()
                        .map(|attrs| Item { attributes: attrs }),
                );
            }

            last_evaluated_key = response.last_evaluated_key;

            if last_evaluated_key.is_none() {
                break;
            }
        }

        Ok(items)
    }

    /// Performs a flexible query operation on a DynamoDB table.
    ///
    /// This method provides full control over the query operation, allowing you to specify
    /// all major query parameters.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to query.
    /// * `key_condition_expression` - A string representing the key condition expression.
    /// * `filter_expression` - An optional filter expression to apply to the results.
    /// * `projection_expression` - An optional projection expression to limit the attributes returned.
    /// * `expression_attribute_names` - A map of expression attribute names used in the key condition and filter expressions.
    /// * `expression_attribute_values` - A map of expression attribute values used in the key condition and filter expressions.
    /// * `limit` - An optional limit on the number of items to evaluate.
    /// * `scan_index_forward` - An optional boolean to specify the scan direction.
    /// * `index_name` - An optional name of a secondary index to query.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of `Item`s if successful, or an error if the operation fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// let items = client.query_flexible(
    ///     "users",
    ///     "#pk = :pkval AND #sk BETWEEN :start AND :end",
    ///     Some("age > :min_age"),
    ///     Some("user_id, username, email"),
    ///     Some(HashMap::from([
    ///         ("#pk".to_string(), "user_id".to_string()),
    ///         ("#sk".to_string(), "created_at".to_string()),
    ///     ])),
    ///     Some(HashMap::from([
    ///         (":pkval".to_string(), AttributeValue::S("123".to_string())),
    ///         (":start".to_string(), AttributeValue::S("2023-01-01".to_string())),
    ///         (":end".to_string(), AttributeValue::S("2023-12-31".to_string())),
    ///         (":min_age".to_string(), AttributeValue::N("18".to_string())),
    ///     ])),
    ///     Some(50),
    ///     Some(false),
    ///     Some("user_index")
    /// ).await?;
    /// ```

    pub async fn query_flexible(&self, params: QueryFlexibleParams<'_>) -> Result<Vec<Item>> {
        let mut query = self
            .client
            .query()
            .table_name(params.table_name)
            .key_condition_expression(params.key_condition_expression)
            .set_expression_attribute_names(params.expression_attribute_names)
            .set_expression_attribute_values(params.expression_attribute_values);

        if let Some(filter) = params.filter_expression {
            query = query.filter_expression(filter);
        }

        if let Some(projection) = params.projection_expression {
            query = query.projection_expression(projection);
        }

        if let Some(limit_val) = params.limit {
            query = query.limit(limit_val);
        }

        if let Some(forward) = params.scan_index_forward {
            query = query.scan_index_forward(forward);
        }

        if let Some(index) = params.index_name {
            query = query.index_name(index);
        }

        let response = query.send().await?;

        Ok(response
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|attrs| Item { attributes: attrs })
            .collect())
    }

    /// Performs a simple query operation on a DynamoDB table.
    ///
    /// This method provides a simplified interface for common query operations,
    /// supporting partition key, optional sort key condition, and basic filtering.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to query.
    /// * `partition_key` - A tuple containing the partition key name and value.
    /// * `sort_key_condition` - An optional tuple containing the sort key name, condition, and value.
    /// * `filter_expression` - An optional filter expression to apply to the results.
    /// * `limit` - An optional limit on the number of items to evaluate.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of `Item`s if successful, or an error if the operation fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// let items = client.query_simple(
    ///     "users",
    ///     ("user_id", AttributeValue::S("123".to_string())),
    ///     Some(("created_at", ">", AttributeValue::S("2023-01-01".to_string()))),
    ///     Some("age > :min_age"),
    ///     Some(10)
    /// ).await?;
    /// ```
    pub async fn query_simple(
        &self,
        table_name: &str,
        partition_key: (&str, AttributeValue),
        sort_key_condition: Option<(&str, String, AttributeValue)>,
        filter_expression: Option<&str>,
        limit: Option<i32>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Result<Vec<Item>> {
        let mut key_condition_expression = "#pk = :pkval".to_string();
        let mut expression_attribute_names =
            HashMap::from([("#pk".to_string(), partition_key.0.to_string())]);
        let mut expression_attribute_values = expression_attribute_values.unwrap_or_default();
        expression_attribute_values.insert(":pkval".to_string(), partition_key.1);

        if let Some((sort_key, condition, value)) = sort_key_condition {
            key_condition_expression.push_str(&format!(" AND #sk {} :skval", condition));
            expression_attribute_names.insert("#sk".to_string(), sort_key.to_string());
            expression_attribute_values.insert(":skval".to_string(), value);
        }

        self.query_flexible(QueryFlexibleParams {
            table_name,
            key_condition_expression: &key_condition_expression,
            expression_attribute_names: Some(expression_attribute_names),
            expression_attribute_values: Some(expression_attribute_values),
            filter_expression,
            projection_expression: None,
            limit,
            scan_index_forward: None,
            index_name: None,
        })
        .await
    }

    /// Performs a scan operation on a DynamoDB table with pagination.
    ///
    /// This method allows for scanning a table with support for filtering, projection,
    /// and pagination.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to scan.
    /// * `filter_expression` - An optional filter expression to apply to the results.
    /// * `projection_expression` - An optional projection expression to limit the attributes returned.
    /// * `expression_attribute_names` - A map of expression attribute names used in the filter expression.
    /// * `expression_attribute_values` - A map of expression attribute values used in the filter expression.
    /// * `limit` - An optional limit on the number of items to evaluate.
    /// * `exclusive_start_key` - An optional exclusive start key for pagination.
    ///
    /// # Returns
    ///
    /// A `Result` containing a tuple of a vector of `Item`s and an optional last evaluated key
    /// for pagination, or an error if the operation fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// let (items, last_key) = client.scan_paginated(
    ///     "users",
    ///     Some("age > :min_age"),
    ///     Some("user_id, username, email"),
    ///     Some(HashMap::new()),
    ///     Some(HashMap::from([(":min_age".to_string(), AttributeValue::N("18".to_string()))])),
    ///     Some(100),
    ///     None
    /// ).await?;
    /// ```
    pub async fn scan_paginated(
        &self,
        table_name: &str,
        filter_expression: Option<&str>,
        projection_expression: Option<&str>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        limit: Option<i32>,
        exclusive_start_key: Option<HashMap<String, AttributeValue>>,
    ) -> Result<(Vec<Item>, Option<HashMap<String, AttributeValue>>)> {
        let scan = self
            .client
            .scan()
            .table_name(table_name)
            .set_filter_expression(filter_expression.map(|s| s.to_string()))
            .set_projection_expression(projection_expression.map(|s| s.to_string()))
            .set_expression_attribute_names(expression_attribute_names)
            .set_expression_attribute_values(expression_attribute_values)
            .set_limit(limit)
            .set_exclusive_start_key(exclusive_start_key);

        let response = scan.send().await?;

        let items = response
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|attrs| Item { attributes: attrs })
            .collect();

        Ok((items, response.last_evaluated_key))
    }
}

pub struct QueryFlexibleParams<'a> {
    pub table_name: &'a str,
    pub key_condition_expression: &'a str,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub filter_expression: Option<&'a str>,
    pub projection_expression: Option<&'a str>,
    pub limit: Option<i32>,
    pub scan_index_forward: Option<bool>,
    pub index_name: Option<&'a str>,
}
