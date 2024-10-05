use crate::dynamodb::Schema;

/// DynamoDB table configuration.
///
/// This struct represents a specific DynamoDB table and its key attributes.
/// Each DynamoDB table is a collection of items (rows) with a primary key.
///
/// # Table Structure
///
/// - **Table Name**: A unique identifier for the table within your AWS account and region.
/// - **Primary Key**: Consists of a partition key and an optional sort key.
///   - **Partition Key**: Determines the partition where the item is stored.
///   - **Sort Key**: Optional. Used to sort items with the same partition key.
/// - **Attributes**: Additional data fields for each item in the table.
///
/// # Table Capacity Modes
///
/// DynamoDB tables can be created in one of two capacity modes:
/// - **Provisioned**: You specify the number of reads and writes per second.
/// - **On-Demand**: DynamoDB instantly accommodates workloads as they ramp up or down.
///
/// # Secondary Indexes
///
/// Tables can have secondary indexes for flexible querying:
/// - **Global Secondary Index (GSI)**: An index with a partition key and sort key that can be different from the table's.
/// - **Local Secondary Index (LSI)**: An index that has the same partition key as the table, but a different sort key.
///
/// # Example
///
/// ```
/// use dynamodb::{Table, Schema, FieldType};
///
/// let schema = Schema::new()
///     .add_field("user_id", FieldType::String)
///     .add_field("timestamp", FieldType::Number)
///     .add_field("message", FieldType::String);
///
/// let table = Table::new("user_messages", "user_id", Some("timestamp"))
///     .with_schema(schema);
/// ```
#[derive(Debug)]
pub struct Table<'a> {
    name: &'a str,
    partition_key: &'a str,
    sort_key: Option<&'a str>,
    schema: Option<Schema>,
}

impl<'a> Table<'a> {
    /// Creates a new `Table` instance.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the DynamoDB table.
    /// * `partition_key` - The name of the partition key attribute.
    /// * `sort_key` - The name of the sort key attribute, if any.
    ///
    /// # Returns
    ///
    /// A new `Table` instance with the specified configuration.
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
    ///
    /// # Arguments
    ///
    /// * `schema` - The `Schema` instance defining the table's attribute structure.
    ///
    /// # Returns
    ///
    /// The modified `Table` instance with the new schema.
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Returns a reference to the table's schema, if set.
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }
}
