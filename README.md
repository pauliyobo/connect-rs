# connect-rs
This library is a simple interface to kafka connect's API.
## Example
There is currently no published version on crates.io

```
[dependencies]
connect-rs = { git = "https://github.com/pauliyobo/connect-rs.git" }
```


```rust
use connect_rs::Connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Connect::new("http://connect-api:8083", "user", Some("password"));
    println!("{:?}", client.connector_names().await?);
    Ok(())
}
```
