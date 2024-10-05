use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::{
    operation::create_table::CreateTableOutput,
    types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType},
    Client,
};
use tracing::{error, info};

pub struct DynamoDbApp {
    client: Client,
}

impl DynamoDbApp {
    pub fn new(sdk_config: &aws_config::SdkConfig) -> Self {
        let client = aws_sdk_dynamodb::Client::new(sdk_config);
        Self { client }
    }

    pub async fn check_authentication(&self) -> Result<()> {
        match self.client.list_tables().send().await {
            Ok(_) => {
                info!("Authentication successful. Credentials are valid.");
                Ok(())
            }
            Err(e) => {
                error!("Authentication failed. Error: {}", e);
                Err(anyhow!("Authentication failed"))
            }
        }
    }

    pub async fn create_table_if_not_exists(
        &self,
        table_name: &str,
        partition_key: &str,
        sort_key: Option<&str>,
    ) -> Result<CreateTableOutput> {
        if self.table_exists(table_name).await? {
            info!("Table '{}' already exists", table_name);
            return Err(anyhow!("Table already exists"));
        }

        let mut attribute_definitions = vec![AttributeDefinition::builder()
            .attribute_name(partition_key)
            .attribute_type(ScalarAttributeType::S)
            .build()?];

        let mut key_schema = vec![KeySchemaElement::builder()
            .attribute_name(partition_key)
            .key_type(KeyType::Hash)
            .build()?];

        if let Some(sort_key) = sort_key {
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
}
