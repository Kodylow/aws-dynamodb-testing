use crate::dynamodb::{DynamoDb, FieldType, Item, QueryFlexibleParams, Table};
use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;
use std::io::{self, Write};
use tracing::info;

/// Runs the command-line interface for interacting with a DynamoDB table.
///
/// This function enters a loop that prompts the user for commands and executes them.
/// The supported commands are:
/// - info: Print table information
/// - put: Add a new item to the table
/// - get: Retrieve an item from the table
/// - update: Update an existing item in the table
/// - delete: Delete an item from the table
/// - query: Query items from the table
/// - scan: Scan items from the table
/// - list: List all items in the table
/// - query_flexible: Perform a flexible query operation with full control over all query parameters
/// - query_simple: Provide a simplified interface for common query operations
/// - scan_paginated: Enable users to perform a paginated scan operation on the table
/// - delete_table: Delete the DynamoDB table
/// - exit: Exit the program
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the function completes successfully, or an error if any operation fails.
pub async fn run(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    loop {
        let command = prompt("Enter command (info/put/get/update/delete/query/scan/list/query_flexible/query_simple/scan_paginated/delete_table/exit): ", None)?;
        match command.as_str() {
            "info" => print_info(ddb, table).await?,
            "put" => put_item(ddb, table).await?,
            "get" => get_item(ddb, table).await?,
            "update" => update_item(ddb, table).await?,
            "delete" => delete_item(ddb, table).await?,
            "query" => query_items(ddb, table).await?,
            "scan" => scan_items(ddb, table).await?,
            "list" => list_items(ddb, table).await?,
            "query_flexible" => query_flexible_items(ddb, table).await?,
            "query_simple" => query_simple_items(ddb, table).await?,
            "scan_paginated" => scan_paginated_items(ddb, table).await?,
            "delete_table" => delete_table(ddb, table).await?,
            "exit" => break,
            _ => println!("Unknown command. Please try again."),
        }
    }
    Ok(())
}

/// Prints detailed information about the DynamoDB table.
///
/// This function retrieves and displays the following information:
/// - Table name
/// - Partition key
/// - Sort key (if present)
/// - Schema (if defined)
/// - Item count
/// - Table size in bytes
/// - Table status
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the function completes successfully, or an error if any operation fails.
async fn print_info(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let table_info = ddb.describe_table(table.name()).await?;
    let items = ddb.scan_table(table.name()).await?;

    println!("\n--- Table Information ---");
    println!("Table Name: {}", table.name());
    println!("Partition Key: {}", table.partition_key());
    if let Some(key) = table.sort_key() {
        println!("Sort Key: {}", key);
    }

    if let Some(schema) = table.schema() {
        println!("Schema:");
        for (field, field_type) in schema.fields() {
            println!("  {}: {:?}", field, field_type);
        }
    }

    let item_count = items.len();
    let table_size_bytes: usize = items
        .iter()
        .flat_map(|item| item.values())
        .map(|attr| {
            attr.as_s()
                .map(|s| s.len())
                .unwrap_or_else(|_| attr.as_n().map_or(0, |n| n.len()))
        })
        .sum();

    println!("Item Count: {}", item_count);
    println!("Table Size (bytes): {}", table_size_bytes);
    println!(
        "Table Status: {:?}",
        table_info.table().unwrap().table_status()
    );
    println!("-------------------------\n");
    Ok(())
}

/// Adds a new item to the DynamoDB table.
///
/// This function prompts the user to enter values for each field defined in the table's schema,
/// creates a new Item, and adds it to the table.
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the item is added successfully, or an error if the operation fails.
async fn put_item(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let schema = table
        .schema()
        .ok_or_else(|| anyhow!("Table schema not defined"))?;
    let item = schema
        .fields()
        .iter()
        .fold(Item::new(), |item, (field_name, field_type)| {
            let value = prompt(&format!("Enter {}: ", field_name), None).unwrap();
            match field_type {
                FieldType::String => item.set_string(field_name, value),
                FieldType::Number => item.set_number(field_name, value.parse::<f64>().unwrap()),
            }
        });

    ddb.put_item(table.name(), item).await?;
    info!("Item added successfully!");
    Ok(())
}

/// Retrieves an item from the DynamoDB table.
///
/// This function prompts the user to enter the key values for the item,
/// retrieves the item from the table, and displays it if found.
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the operation completes successfully, or an error if it fails.
async fn get_item(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let key = create_key_item(table)?;
    match ddb.get_item(table.name(), key).await? {
        Some(item) => println!("Item found: {:?}", item),
        None => println!("Item not found"),
    }
    Ok(())
}

/// Updates an existing item in the DynamoDB table.
///
/// This function prompts the user to enter the key values for the item to update,
/// then prompts for new values for each updateable field. It then sends an update
/// request to DynamoDB with the new values.
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the item is updated successfully, or an error if the operation fails.
async fn update_item(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let key = create_key_item(table)?;
    let updates = create_update_item(table)?;
    ddb.update_item(table.name(), key, updates).await?;
    println!("Item updated successfully!");
    Ok(())
}

/// Deletes an item from the DynamoDB table.
///
/// This function prompts the user to enter the key values for the item to delete,
/// then sends a delete request to DynamoDB for that item.
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the item is deleted successfully, or an error if the operation fails.
async fn delete_item(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let key = create_key_item(table)?;
    ddb.delete_item(table.name(), key).await?;
    println!("Item deleted successfully!");
    Ok(())
}

/// Queries items from the DynamoDB table.
///
/// This function prompts the user to enter query parameters and performs a query operation.
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the query completes successfully, or an error if the operation fails.
async fn query_items(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let partition_key_name = table.partition_key();
    let partition_key_value = prompt(&format!("Enter {} value: ", partition_key_name), None)?;

    let mut key_condition_expression = "#pk = :pkval".to_string();
    let mut expression_attribute_names =
        HashMap::from([("#pk".to_string(), partition_key_name.to_string())]);
    let mut expression_attribute_values =
        HashMap::from([(":pkval".to_string(), AttributeValue::S(partition_key_value))]);

    if let Some(sort_key) = table.sort_key() {
        let sort_key_condition = prompt(
            &format!(
                "Enter condition for {} (e.g., '=', '>', '<', 'BETWEEN'): ",
                sort_key
            ),
            None,
        )?;
        let sort_key_value = prompt(&format!("Enter value for {}: ", sort_key), None)?;

        key_condition_expression.push_str(&format!(" AND #sk {} :skval", sort_key_condition));
        expression_attribute_names.insert("#sk".to_string(), sort_key.to_string());
        expression_attribute_values.insert(":skval".to_string(), AttributeValue::S(sort_key_value));

        if sort_key_condition == "BETWEEN" {
            let sort_key_value_2 = prompt(
                &format!(
                    "Enter second value for {} (for BETWEEN condition): ",
                    sort_key
                ),
                None,
            )?;
            key_condition_expression.push_str(" AND :skval2");
            expression_attribute_values
                .insert(":skval2".to_string(), AttributeValue::S(sort_key_value_2));
        }
    }

    let filter_expression = prompt_optional("Enter filter expression (optional): ", None)?;
    if filter_expression.is_some() {
        let filter_attribute_names = get_expression_attribute_names()?;
        let filter_attribute_values = get_expression_attribute_values()?;
        expression_attribute_names.extend(filter_attribute_names);
        expression_attribute_values.extend(filter_attribute_values);
    }

    let limit = prompt_optional("Enter limit (optional): ", None)?.and_then(|s| s.parse().ok());

    let params = QueryFlexibleParams {
        table_name: table.name(),
        key_condition_expression: &key_condition_expression,
        expression_attribute_names: Some(expression_attribute_names),
        expression_attribute_values: Some(expression_attribute_values),
        filter_expression: filter_expression.as_deref(),
        projection_expression: None,
        limit,
        scan_index_forward: None,
        index_name: None,
    };

    let items = ddb.query_flexible(params).await?;

    print_items(
        "Query Results",
        &items
            .iter()
            .map(|item| item.attributes.clone())
            .collect::<Vec<_>>(),
    );
    Ok(())
}

/// Scans items from the DynamoDB table.
///
/// This function prompts the user to enter an optional filter expression and attribute values,
/// then performs a scan operation on the table and displays the results.
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the scan completes successfully, or an error if the operation fails.
async fn scan_items(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let filter_expression = prompt(
        "Enter filter expression (or press Enter for no filter, e.g., 'attribute_name > :value'): ",
        None,
    )?;

    let (expression_attribute_names, expression_attribute_values) = if !filter_expression.is_empty()
    {
        (
            get_expression_attribute_names()?,
            get_expression_attribute_values()?,
        )
    } else {
        (HashMap::new(), HashMap::new())
    };

    let items = ddb
        .scan(
            table.name(),
            Some(filter_expression),
            Some(expression_attribute_names),
            Some(expression_attribute_values),
        )
        .await?;

    print_items(
        "Scan Results",
        &items
            .iter()
            .map(|item| item.attributes.clone())
            .collect::<Vec<_>>(),
    );
    Ok(())
}

/// Creates an Item containing the key attributes for a DynamoDB operation.
///
/// This function prompts the user to enter values for the partition key and sort key (if present).
///
/// # Arguments
///
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns a Result containing the created Item if successful, or an error if the operation fails.
fn create_key_item(table: &Table<'_>) -> Result<Item> {
    let mut key = Item::new();
    key = key.set_string(
        table.partition_key(),
        prompt(&format!("Enter {}: ", table.partition_key()), None)?,
    );
    if let Some(sort_key) = table.sort_key() {
        key = key.set_string(sort_key, prompt(&format!("Enter {}: ", sort_key), None)?);
    }
    Ok(key)
}

/// Creates an Item containing the attributes to update for a DynamoDB operation.
///
/// This function prompts the user to enter new values for each updateable field in the table schema.
///
/// # Arguments
///
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns a Result containing the created Item if successful, or an error if the operation fails.
fn create_update_item(table: &Table<'_>) -> Result<Item> {
    let schema = table
        .schema()
        .ok_or_else(|| anyhow!("Table schema not defined"))?;
    let mut updates = Item::new();
    for (field_name, field_type) in schema.fields() {
        // Skip partition key and sort key fields
        let is_not_partition_key = field_name != table.partition_key();
        let is_not_sort_key = table
            .sort_key()
            .map_or(true, |sort_key| field_name != sort_key);
        if is_not_partition_key
            && is_not_sort_key
            && prompt(&format!("Update {}? (y/n): ", field_name), None)?.to_lowercase() == "y"
        {
            let value = prompt(&format!("Enter new value for {}: ", field_name), None)?;
            updates = match field_type {
                FieldType::String => updates.set_string(field_name, value),
                FieldType::Number => updates.set_number(field_name, value.parse::<f64>()?),
            };
        }
    }
    Ok(updates)
}

/// Lists all items in the DynamoDB table.
///
/// This function retrieves all items from the table using a scan operation and displays them.
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the operation completes successfully, or an error if it fails.
async fn list_items(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let items = ddb.scan_table(table.name()).await?;
    println!("\n--- Items in {} ---", table.name());
    items.iter().for_each(|item| println!("{:?}", item));
    println!("-------------------------\n");
    Ok(())
}

/// Prompts the user for input and returns the entered string.
///
/// This function displays a message to the user, waits for input, and returns the entered string.
///
/// # Arguments
///
/// * `message` - The message to display to the user
///
/// # Returns
///
/// Returns a Result containing the user's input as a String if successful, or an error if the operation fails.
fn prompt(message: &str, example: Option<&str>) -> Result<String> {
    let full_message = if let Some(ex) = example {
        format!("{} (e.g., {}): ", message, ex)
    } else {
        format!("{}: ", message)
    };
    print!("{}", full_message);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

fn get_expression_attribute_names() -> Result<HashMap<String, String>> {
    let mut names = HashMap::new();
    loop {
        let name = prompt(
            "Enter attribute name (or press Enter to finish)",
            Some("#n"),
        )?;
        if name.is_empty() {
            break;
        }
        let placeholder = prompt("Enter attribute name placeholder", Some("#name"))?;
        names.insert(placeholder, name);
    }
    Ok(names)
}

fn get_expression_attribute_values() -> Result<HashMap<String, AttributeValue>> {
    let mut values = HashMap::new();
    loop {
        let placeholder = prompt(
            "Enter value placeholder (or press Enter to finish)",
            Some(":v"),
        )?;
        if placeholder.is_empty() {
            break;
        }
        let value_type = prompt("Enter value type (S for string, N for number)", Some("S"))?;
        let value = prompt("Enter value", Some("example_value"))?;
        let attribute_value = match value_type.as_str() {
            "S" => AttributeValue::S(value),
            "N" => AttributeValue::N(value),
            _ => return Err(anyhow!("Unsupported value type")),
        };
        values.insert(placeholder, attribute_value);
    }
    Ok(values)
}

/// Performs a flexible query operation on the DynamoDB table.
async fn query_flexible_items(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let key_condition_expression =
        prompt("Enter key condition expression", Some("partitionKey = :pk"))?;
    let filter_expression = prompt_optional("Enter filter expression", Some("attribute > :value"))?;
    let projection_expression =
        prompt_optional("Enter projection expression", Some("attr1, attr2, attr3"))?;

    let expression_attribute_names = get_expression_attribute_names()?;
    let expression_attribute_values = get_expression_attribute_values()?;

    let limit = prompt_optional("Enter limit", Some("10"))?.and_then(|s| s.parse().ok());

    let scan_index_forward = prompt_bool("Scan index forward?", true)?;

    let index_name = prompt_optional("Enter index name", Some("GSI1"))?;

    let params = QueryFlexibleParams {
        table_name: table.name(),
        key_condition_expression: &key_condition_expression,
        expression_attribute_names: Some(expression_attribute_names),
        expression_attribute_values: Some(expression_attribute_values),
        filter_expression: filter_expression.as_deref(),
        projection_expression: projection_expression.as_deref(),
        limit,
        scan_index_forward: Some(scan_index_forward),
        index_name: index_name.as_deref(),
    };

    let items = ddb.query_flexible(params).await?;

    print_items(
        "Query Flexible Results",
        &items
            .iter()
            .map(|item| item.attributes.clone())
            .collect::<Vec<_>>(),
    );
    Ok(())
}

/// Performs a simple query operation on the DynamoDB table.
async fn query_simple_items(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let partition_key_name = table.partition_key();
    let partition_key_value = prompt(
        &format!("Enter {} value", partition_key_name),
        Some("example_value"),
    )?;
    let partition_key = (partition_key_name, AttributeValue::S(partition_key_value));

    let sort_key_condition = table.sort_key().map(|sort_key| {
        let condition = prompt(
            &format!(
                "Enter condition for {} (e.g., '>', '<', '=', 'BETWEEN')",
                sort_key
            ),
            Some(">="),
        )
        .unwrap();
        let value = prompt(
            &format!("Enter value for {}", sort_key),
            Some("example_value"),
        )
        .unwrap();
        (sort_key, condition, AttributeValue::S(value))
    });

    let filter_expression = prompt_optional("Enter filter expression", Some("attribute > :value"))?;

    let limit = prompt_optional("Enter limit", Some("10"))?.and_then(|s| s.parse().ok());

    let expression_attribute_values = get_expression_attribute_values()?;

    let items = ddb
        .query_simple(
            table.name(),
            partition_key,
            sort_key_condition,
            filter_expression.as_deref(),
            limit,
            Some(expression_attribute_values),
        )
        .await?;

    print_items(
        "Query Simple Results",
        &items
            .iter()
            .map(|item| item.attributes.clone())
            .collect::<Vec<_>>(),
    );
    Ok(())
}

/// Performs a paginated scan operation on the DynamoDB table.
async fn scan_paginated_items(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let filter_expression = prompt_optional("Enter filter expression", Some("attribute > :value"))?;
    let projection_expression =
        prompt_optional("Enter projection expression", Some("attr1, attr2, attr3"))?;

    let expression_attribute_names = get_expression_attribute_names()?;
    let expression_attribute_values = get_expression_attribute_values()?;
    let filter_expression = match filter_expression {
        Some(expr) if !expr.is_empty() => Some(expr),
        _ => None,
    };

    let projection_expression = match projection_expression {
        Some(expr) if !expr.is_empty() => Some(expr),
        _ => None,
    };

    let limit = prompt("Enter limit (or press Enter for none)", Some("10"))?;
    let limit = if limit.is_empty() {
        None
    } else {
        Some(limit.parse()?)
    };

    let mut exclusive_start_key = None;
    let mut page_num = 1;

    loop {
        let (items, last_evaluated_key) = ddb
            .scan_paginated(
                table.name(),
                filter_expression.as_deref(),
                projection_expression.as_deref(),
                Some(expression_attribute_names.clone()),
                Some(expression_attribute_values.clone()),
                limit,
                exclusive_start_key.clone(),
            )
            .await?;

        print_items(
            &format!("Scan Paginated Results (Page {})", page_num),
            &items
                .iter()
                .map(|item| item.attributes.clone())
                .collect::<Vec<_>>(),
        );

        if last_evaluated_key.is_none() {
            break;
        }

        let continue_scan = prompt("Continue to next page? (y/n)", Some("y"))?;
        if continue_scan.to_lowercase() != "y" {
            break;
        } else {
            exclusive_start_key = last_evaluated_key;
            page_num += 1;
        }
    }

    Ok(())
}

fn print_items(title: &str, items: &[HashMap<String, AttributeValue>]) {
    println!("\n--- {} ---", title);
    items.iter().for_each(|item| println!("{:?}", item));
    println!("{}", "-".repeat(title.len() + 8));
}

fn prompt_optional(message: &str, example: Option<&str>) -> Result<Option<String>> {
    let input = prompt(message, example)?;
    Ok(if input.is_empty() { None } else { Some(input) })
}

fn prompt_bool(message: &str, default: bool) -> Result<bool> {
    let input = prompt(
        &format!("{} (y/n)", message),
        Some(if default { "y" } else { "n" }),
    )?;
    Ok(input.to_lowercase().starts_with('y') || (input.is_empty() && default))
}

/// Deletes the DynamoDB table.
///
/// This function prompts the user for confirmation before deleting the table.
///
/// # Arguments
///
/// * `ddb` - A reference to the DynamoDB client
/// * `table` - A reference to the Table struct containing table information
///
/// # Returns
///
/// Returns `Ok(())` if the table is deleted successfully, or an error if the operation fails.
async fn delete_table(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let confirmation = prompt(
        &format!(
            "Are you sure you want to delete the table '{}'? This action cannot be undone. (y/n): ",
            table.name()
        ),
        None,
    )?;

    if confirmation.to_lowercase() == "y" {
        ddb.delete_table(table.name()).await?;
        println!("Table '{}' has been deleted.", table.name());
    } else {
        println!("Table deletion cancelled.");
    }

    Ok(())
}
