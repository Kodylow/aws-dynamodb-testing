use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::{
    operation::create_table::CreateTableOutput,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
    Client,
};
use std::collections::HashMap;
use tracing::{error, info};

pub struct DynamoDb {
    client: Client,
}

#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    partition_key: String,
    sort_key: Option<String>,
}

impl Table {
    pub fn new<S: Into<String>>(name: S, partition_key: S, sort_key: Option<S>) -> Self {
        Self {
            name: name.into(),
            partition_key: partition_key.into(),
            sort_key: sort_key.map(Into::into),
        }
    }
}

impl DynamoDb {
    pub fn new(sdk_config: &aws_config::SdkConfig) -> Self {
        Self {
            client: Client::new(sdk_config),
        }
    }

    pub async fn check_auth(&self) -> Result<()> {
        self.client.list_tables().send().await.map_err(|e| {
            error!("Authentication failed. Error: {}", e);
            anyhow!("Authentication failed")
        })?;
        info!("Authentication successful. Credentials are valid.");
        Ok(())
    }

    pub async fn create_table_if_not_exists(&self, table: &Table) -> Result<CreateTableOutput> {
        if self.table_exists(&table.name).await? {
            info!("Table '{}' already exists", table.name);
            return self.describe_table(&table.name).await;
        }

        let mut attribute_definitions = vec![AttributeDefinition::builder()
            .attribute_name(&table.partition_key)
            .attribute_type(ScalarAttributeType::S)
            .build()?];

        let mut key_schema = vec![KeySchemaElement::builder()
            .attribute_name(&table.partition_key)
            .key_type(KeyType::Hash)
            .build()?];

        if let Some(sort_key) = &table.sort_key {
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

        self.client
            .create_table()
            .table_name(&table.name)
            .billing_mode(BillingMode::PayPerRequest)
            .set_attribute_definitions(Some(attribute_definitions))
            .set_key_schema(Some(key_schema))
            .send()
            .await
            .map_err(Into::into)
    }

    pub async fn put_item<S: AsRef<str>>(&self, table_name: S, item: Item) -> Result<()> {
        self.client
            .put_item()
            .table_name(table_name.as_ref())
            .set_item(Some(item.into_attributes()))
            .send()
            .await?;

        info!("Item successfully put into table '{}'", table_name.as_ref());
        Ok(())
    }

    async fn table_exists<S: AsRef<str>>(&self, table_name: S) -> Result<bool> {
        let tables = self.client.list_tables().send().await?;
        Ok(tables
            .table_names()
            .contains(&table_name.as_ref().to_string()))
    }

    async fn describe_table<S: AsRef<str>>(&self, table_name: S) -> Result<CreateTableOutput> {
        let describe_table_output = self
            .client
            .describe_table()
            .table_name(table_name.as_ref())
            .send()
            .await?;

        Ok(CreateTableOutput::builder()
            .table_description(describe_table_output.table().unwrap().to_owned())
            .build())
    }
}

#[derive(Default)]
pub struct Item {
    attributes: HashMap<String, AttributeValue>,
}

impl Item {
    pub fn new() -> Self {
        Item {
            attributes: HashMap::new(),
        }
    }

    pub fn set_string<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.attributes
            .insert(key.into(), AttributeValue::S(value.into()));
        self
    }

    pub fn set_number<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.attributes
            .insert(key.into(), AttributeValue::N(value.into()));
        self
    }

    // pub fn set_bool(mut self, key: impl Into<String>, value: bool) -> Self {
    //     self.attributes
    //         .insert(key.into(), AttributeValue::Bool(value));
    //     self
    // }

    // pub fn set_list(mut self, key: impl Into<String>, value: Vec<AttributeValue>) -> Self {
    //     self.attributes.insert(key.into(), AttributeValue::L(value));
    //     self
    // }

    pub fn into_attributes(self) -> HashMap<String, AttributeValue> {
        self.attributes
    }
}
