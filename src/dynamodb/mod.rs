//! # DynamoDB Module
//!
//! This module provides a high-level interface for interacting with Amazon DynamoDB.
//!
//! ## Components
//!
//! - `DynamoDb`: A client wrapper for performing DynamoDB operations.
//! - `Item`: Represents a DynamoDB item with various attribute types.
//! - `Schema`: Defines the structure of a DynamoDB table.
//! - `Table`: Represents a DynamoDB table configuration.
//!
//! ## Usage
//!
//! To use this module, you need to set up the following environment variables:
//!
//! - `AWS_ACCESS_KEY_ID`: Your AWS access key ID.
//! - `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key.
//! - `AWS_REGION`: The AWS region where your DynamoDB tables are located.
//!
//! Optionally, you can also set:
//! - `AWS_SESSION_TOKEN`: If you're using temporary credentials.
//! - `AWS_ENDPOINT_URL`: For using a custom endpoint (e.g., for local development).
//!
//! ## Example
//!
//! ```rust
//! use aws_config::load_from_env;
//! use dynamodb::{DynamoDb, Table, Item, Schema, FieldType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Load AWS configuration from environment variables
//!     let config = load_from_env().await;
//!     
//!     // Create a DynamoDB client
//!     let client = DynamoDb::new(&config);
//!
//!     // Define a table schema
//!     let schema = Schema::new()
//!         .add_field("user_id", FieldType::String)
//!         .add_field("email", FieldType::String)
//!         .add_field("name", FieldType::String);
//!
//!     // Create a table configuration
//!     let table = Table::new("users", "user_id", None)
//!         .with_schema(schema);
//!
//!     // Create the table if it doesn't exist
//!     client.create_table_if_not_exists(&table).await?;
//!
//!     // Create an item
//!     let item = Item::new()
//!         .set_string("user_id", "123")
//!         .set_string("email", "user@example.com")
//!         .set_string("name", "John Doe");
//!
//!     // Put the item into the table
//!     client.put_item("users", item).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! This module simplifies DynamoDB operations and provides a more Rust-idiomatic interface
//! to work with DynamoDB tables and items.

mod client;
mod item;
mod schema;
mod table;

pub use client::DynamoDb;
pub use item::Item;
pub use schema::{FieldType, Schema};
pub use table::Table;
