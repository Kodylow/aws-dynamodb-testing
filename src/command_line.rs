use crate::dynamodb::{DynamoDb, FieldType, Item, Table};
use anyhow::{anyhow, Result};
use std::io::{self, Write};
use tracing::info;

pub async fn run(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    loop {
        let command = prompt("Enter command (info/put/list/exit): ")?;
        match command.as_str() {
            "info" => print_info(ddb, table).await?,
            "put" => put_item(ddb, table).await?,
            "list" => list_items(ddb, table).await?,
            "exit" => break,
            _ => println!("Unknown command. Please try again."),
        }
    }
    Ok(())
}

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

async fn list_items(ddb: &DynamoDb, table: &Table<'_>) -> Result<()> {
    let items = ddb.scan_table(table.name()).await?;
    println!("\n--- Items in {} ---", table.name());
    items.iter().for_each(|item| println!("{:?}", item));
    println!("-------------------------\n");
    Ok(())
}

fn prompt(message: &str) -> Result<String> {
    print!("{}", message);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}
