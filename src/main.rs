use aws_sdk_dynamodb as ddb;

#[tokio::main]
async fn main() {
    let sdk_config = aws_config::load_from_env().await;

    let ddb_client = ddb::Client::new(&sdk_config);
    let table_name = "testing-products";
    let resp = ddb_client.create_table().table_name(table_name);
}
