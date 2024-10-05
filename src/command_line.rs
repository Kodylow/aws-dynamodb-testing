use crate::dynamodb::{DynamoDb, FieldType, Item, Table};
use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::types::AttributeValue;
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
/// - list: List all items in the table
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
        let command = prompt("Enter command (info/put/get/update/delete/query/list/exit): ")?;
        match command.as_str() {
            "info" => print_info(ddb, table).await?,
            "put" => put_item(ddb, table).await?,
            "get" => get_item(ddb, table).await?,
            "update" => update_item(ddb, table).await?,
            "delete" => delete_item(ddb, table).await?,
            "query" => query_items(ddb, table).await?,
            "list" => list_items(ddb, table).await?,
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
    table.sort_key().map(|key| println!("Sort Key: {}", key));

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
            let value = prompt(&format!("Enter {}: ", field_name)).unwrap();
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
/// This function prompts the user to enter a partition key value and optionally a sort key condition.
/// It then performs a query operation on the table and displays the results.
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
    let partition_key_value = prompt(&format!("Enter {} value: ", table.partition_key()))?;
    let partition_key = (
        table.partition_key(),
        AttributeValue::S(partition_key_value),
    );

    let sort_key_condition = if let Some(sort_key) = table.sort_key() {
        let condition = prompt("Enter sort key condition (e.g., '= 123', '> 100', '< 200'): ")?;
        let (operator, value) = condition
            .split_once(' ')
            .ok_or_else(|| anyhow!("Invalid condition format"))?;
        Some((
            sort_key,
            operator.to_string(),
            AttributeValue::S(value.to_string()),
        ))
    } else {
        None
    };

    let items = ddb
        .query_items(table.name(), partition_key, sort_key_condition)
        .await?;
    println!("\n--- Query Results ---");
    items.iter().for_each(|item| println!("{:?}", item));
    println!("---------------------\n");
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
        prompt(&format!("Enter {}: ", table.partition_key()))?,
    );
    if let Some(sort_key) = table.sort_key() {
        key = key.set_string(sort_key, prompt(&format!("Enter {}: ", sort_key))?);
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
        if is_not_partition_key && is_not_sort_key {
            if prompt(&format!("Update {}? (y/n): ", field_name))?.to_lowercase() == "y" {
                let value = prompt(&format!("Enter new value for {}: ", field_name))?;
                updates = match field_type {
                    FieldType::String => updates.set_string(field_name, value),
                    FieldType::Number => updates.set_number(field_name, value.parse::<f64>()?),
                };
            }
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
fn prompt(message: &str) -> Result<String> {
    print!("{}", message);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}
