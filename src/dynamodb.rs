use std::collections::HashMap;

use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::{
    operation::{create_table::CreateTableOutput, put_item::PutItemOutput},
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
    Client,
};
use tracing::{error, info};

pub struct DynamoDbApp {
    client: Client,
}

impl DynamoDbApp {
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

    pub async fn create_table_if_not_exists(
        &self,
        table_name: &str,
        partition_key: &str,
        sort_key: Option<&str>,
    ) -> Result<CreateTableOutput> {
        if self.table_exists(table_name).await? {
            info!("Table '{}' already exists", table_name);
            return self.describe_table(table_name).await;
        }

        let mut attribute_definitions = vec![build_attribute_definition(partition_key)?];
        let mut key_schema = vec![build_key_schema_element(partition_key, KeyType::Hash)?];

        if let Some(sort_key) = sort_key {
            attribute_definitions.push(build_attribute_definition(sort_key)?);
            key_schema.push(build_key_schema_element(sort_key, KeyType::Range)?);
        }

        let create_table_output = self
            .client
            .create_table()
            .table_name(table_name)
            .billing_mode(BillingMode::PayPerRequest)
            .set_attribute_definitions(Some(attribute_definitions))
            .set_key_schema(Some(key_schema))
            .send()
            .await?;

        Ok(create_table_output)
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let tables = self.client.list_tables().send().await?;
        Ok(tables.table_names().contains(&table_name.to_string()))
    }

    pub async fn put_item(
        &self,
        table_name: &str,
        item: HashMap<String, AttributeValue>,
    ) -> Result<PutItemOutput> {
        let put_item_output = self
            .client
            .put_item()
            .table_name(table_name)
            .set_item(Some(item.clone()))
            .send()
            .await?;

        info!("Item {:?} successfully put into table '{table_name}'", item);
        Ok(put_item_output)
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

fn build_attribute_definition(key: &str) -> Result<AttributeDefinition> {
    Ok(AttributeDefinition::builder()
        .attribute_name(key)
        .attribute_type(ScalarAttributeType::S)
        .build()?)
}

fn build_key_schema_element(key: &str, key_type: KeyType) -> Result<KeySchemaElement> {
    Ok(KeySchemaElement::builder()
        .attribute_name(key)
        .key_type(key_type)
        .build()?)
}
