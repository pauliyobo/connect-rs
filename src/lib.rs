pub mod models;
use models::*;
use std::collections::HashMap;

use base64::{engine::general_purpose, Engine};
use reqwest::{header, Client, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::RetryTransientMiddleware;
use retry_policies::policies::ExponentialBackoff;
use retry_policies::Jitter;
use std::time::Duration;
use thiserror::Error;

const ENGINE: general_purpose::GeneralPurpose = general_purpose::STANDARD;

/// ConnectError
#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("A rebalance may be  needed, forthcoming, or underway.")]
    RebalancingInProgress,
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
    #[error("An internal server error occurred while processing the request.")]
    InternalError,
    #[error("At least one of 'info' or 'status' must be expanded.")]
    InvalidExpandOption,
    #[error(transparent)]
    RequestError(#[from] reqwest::Error),
    #[error(transparent)]
    MiddlewareError(#[from] reqwest_middleware::Error),
    #[error("The connector {0} does not exist.")]
    ConnectorNotFound,
}

pub type Result<T> = anyhow::Result<T, ConnectError>;

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
            return Err(ConnectError::InvalidExpandOption);
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
            StatusCode::NOT_FOUND => Err(ConnectError::ConnectorNotFound(name.to_string())),
            StatusCode::CONFLICT => Err(ConnectError::RebalancingInProgress),
            StatusCode::INTERNAL_SERVER_ERROR => Err(ConnectError::InternalError),
            _ => Err(ConnectError::Unknown(anyhow::anyhow!(
                "Unrecognizable error for status code {}",
                status_code
            ))),
        }
    }

    pub async fn delete_connector(&self, connector: &str) -> Result<()> {
        let response = self
            .client
            .delete(format!("{}/connectors/{}", self.address, connector))
            .send()
            .await?;
        let status_code = response.status();
        match status_code {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::CONFLICT => Err(ConnectError::RebalancingInProgress),
            _ => Err(ConnectError::Unknown(anyhow::anyhow!(
                "Unrecognizable error"
            ))),
        }
    }

    pub async fn connector_config(&self, connector: &str) -> Result<HashMap<String, String>> {
        let response: HashMap<String, String> = self
            .client
            .get(format!("{}/connectors/{}/config", self.address, connector))
            .send()
            .await?
            .json()
            .await?;
        Ok(response)
    }

    pub async fn pause_connector(&self, name: &str) -> Result<()> {
        self.client
            .put(format!("{}/connectors/{}/pause", self.address, name))
            .send()
            .await?;
        Ok(())
    }

    pub async fn resume_connector(&self, name: &str) -> Result<()> {
        self.client
            .put(format!("{}/connectors/{}/resume", self.address, name))
            .send()
            .await?;
        Ok(())
    }

    pub async fn stop_connector(&self, name: &str) -> Result<()> {
        self.client
            .put(format!("{}/connectors/{}/stop", self.address, name))
            .send()
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_info() {
        let expected = models::ClusterInfo {
            commit: "test".into(),
            version: "0.1.0".into(),
            kafka_cluster_id: "test".into(),
        };
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/")
            .with_body(serde_json::to_string(&expected).unwrap())
            .with_status(200)
            .create_async()
            .await;
        let connect = Connect::new(&format!("http://{}", server.host_with_port()), "", None);
        let actual = connect.info().await.unwrap();
        assert_eq!(actual.commit, expected.commit)
    }

    #[tokio::test]
    async fn test_expand() {
        let connector = Connector {
            info: Some(ConnectorInfo {
                name: "test".into(),
                config: HashMap::new(),
                tasks: Vec::new(),
                kind: "source".into(),
            }),
            status: None,
        };
        let mut expected = HashMap::new();
        expected.insert("test".to_string(), connector);
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/connectors")
            .match_query(mockito::Matcher::AnyOf(
                vec![
                    mockito::Matcher::Exact("expand=info".to_string()),
                    mockito::Matcher::Exact("expand=status".to_string()),
                ],
            ))
            .with_body(serde_json::to_string(&expected).unwrap())
            .with_status(200)
            .create_async()
            .await;
        let connect = Connect::new(&server.url(), "", None);
        let actual = connect.connectors(false, true).await.unwrap();
        assert_eq!(expected, actual);
        // if both expand_info and expand_status are false, we expect an error
        let actual = connect.connectors(false, false).await;
        assert!(actual.is_err())
    }
}
