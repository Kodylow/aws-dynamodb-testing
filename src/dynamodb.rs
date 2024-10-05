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

#[derive(Debug)]
pub struct Table<'a> {
    name: &'a str,
    partition_key: &'a str,
    sort_key: Option<&'a str>,
}

impl<'a> Table<'a> {
    pub fn new(name: &'a str, partition_key: &'a str, sort_key: Option<&'a str>) -> Self {
        Self {
            name,
            partition_key,
            sort_key,
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

    pub async fn create_table_if_not_exists(&self, table: &Table<'_>) -> Result<CreateTableOutput> {
        if self.table_exists(table.name).await? {
            info!("Table '{}' already exists", table.name);
            return self.describe_table(table.name).await;
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

        self.client
            .create_table()
            .table_name(table.name)
            .billing_mode(BillingMode::PayPerRequest)
            .set_attribute_definitions(Some(attribute_definitions))
            .set_key_schema(Some(key_schema))
            .send()
            .await
            .map_err(Into::into)
    }

    pub async fn put_item(&self, table_name: &str, item: Item) -> Result<()> {
        self.client
            .put_item()
            .table_name(table_name)
            .set_item(Some(item.attributes))
            .send()
            .await?;

        info!("Item successfully put into table '{table_name}'");
        Ok(())
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let tables = self.client.list_tables().send().await?;
        Ok(tables.table_names().contains(&table_name.to_string()))
    }

    async fn describe_table(&self, table_name: &str) -> Result<CreateTableOutput> {
        let describe_table_output = self
            .client
            .describe_table()
            .table_name(table_name)
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
        Self::default()
    }

    pub fn set_string(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes
            .insert(key.into(), AttributeValue::S(value.into()));
        self
    }

    pub fn set_number(mut self, key: impl Into<String>, value: impl Into<f64>) -> Self {
        self.attributes
            .insert(key.into(), AttributeValue::N(value.into().to_string()));
        self
    }
}
