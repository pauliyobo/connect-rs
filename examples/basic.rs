use connect_rs::Connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Connect::new("http://connect-api:8083", "user", Some("password"));
    println!("{:?}", client.connector_names().await?);
    Ok(())
}
