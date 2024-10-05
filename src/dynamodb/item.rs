use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

/// Represents a DynamoDB item with various attribute types.
///
/// In DynamoDB, an item is a collection of attributes, each with a name and a value.
/// Items are similar to rows or records in other database systems.
///
/// # Item Structure
///
/// - Each item consists of one or more attributes.
/// - Each attribute has a name and a value.
/// - Attribute values can be of various types: String, Number, Binary, Boolean, Null, List, Map, etc.
///
/// # Primary Key
///
/// - Every item in a table is uniquely identified by its primary key.
/// - The primary key can be simple (partition key only) or composite (partition key and sort key).
///
/// # Item Size Limit
///
/// - The maximum item size in DynamoDB is 400 KB, including both attribute names and values.
///
/// # Example
///
/// ```
/// use dynamodb::Item;
///
/// let item = Item::new()
///     .set_string("user_id", "12345")
///     .set_string("username", "johndoe")
///     .set_number("age", 30.0);
/// ```
#[derive(Default, Debug, Clone)]
pub struct Item {
    pub(crate) attributes: HashMap<String, AttributeValue>,
}

impl Item {
    /// Creates a new empty `Item`.
    pub fn new() -> Self {
        Self::default()
    }

    // /// Returns the id of the item, if it exists and is a string.
    // pub fn id(&self) -> Option<String> {
    //     self.attributes
    //         .get("id")
    //         .and_then(|attr| attr.as_s().ok())
    //         .map(|s| s.to_string())
    // }

    /// Sets a string attribute.
    ///
    /// In DynamoDB, string attributes are used for text data.
    pub fn set_string(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes
            .insert(key.into(), AttributeValue::S(value.into()));
        self
    }

    /// Sets a number attribute.
    ///
    /// In DynamoDB, number attributes are used for numeric data and are stored with high precision.
    pub fn set_number(mut self, key: impl Into<String>, value: impl Into<f64>) -> Self {
        self.attributes
            .insert(key.into(), AttributeValue::N(value.into().to_string()));
        self
    }

    /// Gets the value of an attribute as a string.
    ///
    /// Returns `None` if the attribute doesn't exist or is not a string.
    #[allow(dead_code)]
    pub fn get_string(&self, key: &str) -> Option<&String> {
        self.attributes.get(key).and_then(|av| av.as_s().ok())
    }

    /// Gets the value of an attribute as a number (f64).
    ///
    /// Returns `None` if the attribute doesn't exist, is not a number, or can't be parsed as f64.
    #[allow(dead_code)]
    pub fn get_number(&self, key: &str) -> Option<f64> {
        self.attributes
            .get(key)
            .and_then(|av| av.as_n().ok())
            .and_then(|n| n.parse().ok())
    }
}
