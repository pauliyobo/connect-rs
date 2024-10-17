pub mod models;
use models::*;
use std::collections::HashMap;

use anyhow::Result;
use base64::{engine::general_purpose, Engine};
use reqwest::{header, Client, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use retry_policies::Jitter;
use std::time::Duration;

const ENGINE: general_purpose::GeneralPurpose = general_purpose::STANDARD;

/// main interface
#[derive(Debug, Clone)]
pub struct Connect {
    client: ClientWithMiddleware,
    address: String,
}

impl Connect {
    pub fn new(address: &str, username: &str, password: Option<&str>) -> Self {
        // set up the basic auth
        let credentials = ENGINE.encode(format!("{}:{}", username, password.unwrap_or("")));
        let basic_auth = format!("Basic {}", credentials);
        let mut headers = header::HeaderMap::new();
        let mut auth_value = header::HeaderValue::from_str(&basic_auth).unwrap();
        auth_value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth_value);
        let client = Client::builder().default_headers(headers).build().unwrap();
        let address = address.to_string();
        // setup backoff
        let policy = ExponentialBackoff::builder()
            .retry_bounds(Duration::from_secs(1), Duration::from_secs(60))
            .jitter(Jitter::Bounded)
            .base(2)
            .build_with_total_retry_duration(Duration::from_secs(600));
        let retry_transient_middleware = RetryTransientMiddleware::new_with_policy(policy);
        let client = ClientBuilder::new(client)
            .with(retry_transient_middleware)
            .build();
        Self { client, address }
    }

    /// Returns info for a kafka-connect cluster
    pub async fn info(&self) -> Result<ClusterInfo> {
        let response: ClusterInfo = self
            .client
            .get(format!("{}/", self.address))
            .send()
            .await?
            .json()
            .await?;
        Ok(response)
    }

    /// Get a list of connector names
    /// Since the API has two flavors, where one just returns the list of names and the other
    /// returns the complete structure, we're differentiating here
    pub async fn connector_names(&self) -> Result<Vec<String>> {
        let response = self
            .client
            .get(format!("{}/connectors", self.address))
            .send()
            .await?
            .json()
            .await?;
        Ok(response)
    }

    pub async fn connectors(
        &self,
        expand_status: bool,
        expand_info: bool,
    ) -> Result<HashMap<String, Connector>> {
        let mut endpoint = format!("{}/connectors", self.address);
        // TODO: Perhaps replace this logic with the URL crate, if at all possible
        let expand = match (expand_status, expand_info) {
            (true, true) => "?expand=status&expand=info",
            (false, true) => "?expand=info",
            (true, false) => "?expand=status",
            (false, false) => "",
        };
        if expand.is_empty() {
            anyhow::bail!("You must expand either info, status, or both. If you'd rather use none of them, you may call the connector_names() method instead");
        }
        endpoint.push_str(expand);
        let response = self.client.get(endpoint).send().await?.json().await?;
        Ok(response)
    }

    pub async fn restart_connector(
        &self,
        name: &str,
        include_tasks: bool,
        only_failed: bool,
    ) -> Result<Option<ConnectorStatus>> {
        let response = self
            .client
            .post(format!(
                "{}/connectors/{}/restart?includeTasks={}&onlyFailed={}",
                self.address, name, include_tasks, only_failed
            ))
            .send()
            .await?;
        let status_code = response.status();
        match status_code {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(None),
            StatusCode::ACCEPTED => Ok(response.json().await?),
            StatusCode::NOT_FOUND => anyhow::bail!("Connector does not exist"),
            StatusCode::CONFLICT => {
                anyhow::bail!("A rebalance may be  needed, forthcoming, or underway")
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                anyhow::bail!("The request could not be processed.")
            }
            _ => anyhow::bail!("Unrecognizable error for status code {}", status_code),
        }
    }

    pub async fn delete_connector(&self, connector: &str) -> anyhow::Result<()> {
        let response = self
            .client
            .delete(format!("{}/connectors/{}", self.address, connector))
            .send()
            .await?;
        let status_code = response.status();
        match status_code {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::CONFLICT => {
                anyhow::bail!("A rebalance may be  needed, forthcoming, or underway")
            }
            _ => anyhow::bail!("Unrecognizable error"),
        }
    }

    pub async fn connector_config(
        &self,
        connector: &str,
    ) -> anyhow::Result<HashMap<String, String>> {
        let response: HashMap<String, String> = self
            .client
            .get(format!("{}/connectors/{}/config", self.address, connector))
            .send()
            .await?
            .json()
            .await?;
        Ok(response)
    }
}
