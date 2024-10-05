use std::collections::HashMap;

/// Represents the schema of a DynamoDB table.
///
/// In DynamoDB, a schema defines the structure of items in a table.
/// Unlike traditional relational databases, DynamoDB is schemaless,
/// meaning you don't need to define a schema before adding data to a table.
/// However, it's often useful to define a schema for your application's use.
///
/// # Schema Components
///
/// - **Attributes**: Each item in a DynamoDB table can have one or more attributes.
/// - **Data Types**: DynamoDB supports several data types for attributes:
///   - Scalar Types: String, Number, Binary, Boolean, Null
///   - Document Types: List, Map
///   - Set Types: String Set, Number Set, Binary Set
///
/// # Primary Key
///
/// Every DynamoDB table must have a primary key, which can be:
/// - **Simple Primary Key**: Consists of just a partition key.
/// - **Composite Primary Key**: Consists of a partition key and a sort key.
///
/// # Secondary Indexes
///
/// DynamoDB supports two types of secondary indexes:
/// - **Global Secondary Index (GSI)**: An index with a partition key and sort key that can be different from the table's.
/// - **Local Secondary Index (LSI)**: An index that has the same partition key as the table, but a different sort key.
///
/// # Example
///
/// ```
/// use dynamodb::{Schema, FieldType};
///
/// let schema = Schema::new()
///     .add_field("user_id", FieldType::String)
///     .add_field("timestamp", FieldType::Number)
///     .add_field("message", FieldType::String);
/// ```
#[derive(Debug, Clone)]
pub struct Schema {
    fields: HashMap<String, FieldType>,
}

/// Represents the type of a field in a DynamoDB table schema.
///
/// DynamoDB supports various data types for attributes. This enum
/// represents a subset of these types commonly used in schemas.
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
