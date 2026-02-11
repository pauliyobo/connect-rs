pub mod models;
use models::*;
use std::collections::HashMap;

use base64::{Engine, engine::general_purpose};
use reqwest::{Client, StatusCode, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::RetryTransientMiddleware;
use retry_policies::{Jitter, policies::ExponentialBackoff};
use std::time::Duration;
use thiserror::Error;

const ENGINE: general_purpose::GeneralPurpose = general_purpose::STANDARD;

/// ConnectError
#[derive(Debug, Error)]
pub enum ConnectError {
    /// The operation returned a bad request
    #[error("[0].message")]
    BadRequest(ErrorMessage),
    /// The operation can not complete because of a rebalance
    #[error("A rebalance may be  needed, forthcoming, or underway.")]
    RebalancingInProgress,
    /// Unknown error
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
    /// Internal server error
    #[error("An internal server error occurred while processing the request.")]
    InternalError,
    #[error("At least one of 'info' or 'status' must be expanded.")]
    InvalidExpandOption,
    /// reqwest error
    #[error(transparent)]
    RequestError(#[from] reqwest::Error),
    // reqwest_middleware error
    #[error(transparent)]
    MiddlewareError(#[from] reqwest_middleware::Error),
    #[error("The connector {0} does not exist.")]
    ConnectorNotFound(String),
}

pub type Result<T> = anyhow::Result<T, ConnectError>;

/// main interface
#[derive(Debug, Clone)]
pub struct Connect {
    client: ClientWithMiddleware,
    address: String,
}

impl Connect {
    pub fn new(address: &str, username: Option<&str>, password: Option<&str>) -> Self {
        let mut headers = header::HeaderMap::new();
        if let Some(username) = username {
            // set up the basic auth
            let credentials = ENGINE.encode(format!("{}:{}", username, password.unwrap_or("")));
            let basic_auth = format!("Basic {}", credentials);
            let mut auth_value = header::HeaderValue::from_str(&basic_auth).unwrap();
            auth_value.set_sensitive(true);
            headers.insert(header::AUTHORIZATION, auth_value);
        }
        let client = Client::builder().default_headers(headers).build().unwrap();
        let address = address.to_string();
        // setup backoff
        let policy = ExponentialBackoff::builder()
            .retry_bounds(Duration::from_secs(1), Duration::from_secs(5))
            .jitter(Jitter::Bounded)
            .base(2)
            .build_with_total_retry_duration(Duration::from_secs(20));
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

    /// return a list of connectors with expanded information
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

    /// create a new connector
    pub async fn create_connector(&self, name: &str, config: HashMap<String, String>) -> Result<ConnectorInfo> {
        let body = serde_json::json!({
            "name": name,
            "config": config
        });
        let response = self.client.post(format!("{}/connectors", self.address)).json(&body).send().await?;
        match response.status() {
            StatusCode::CREATED => Ok(response.json().await?),
            StatusCode::CONFLICT => Err(ConnectError::RebalancingInProgress),
            StatusCode::BAD_REQUEST => Err(ConnectError::BadRequest(response.json().await?)),
            _ => {
                println!("{:?}", response.text().await?);
                Err(ConnectError::InternalError)
            }
        }
    }
    
    /// Restart a connector
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

    /// Delete a connector
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

    /// return config information for a connector
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

    /// pause a connector
    pub async fn pause_connector(&self, name: &str) -> Result<()> {
        self.client
            .put(format!("{}/connectors/{}/pause", self.address, name))
            .send()
            .await?;
        Ok(())
    }

    /// Resume a connector
    pub async fn resume_connector(&self, name: &str) -> Result<()> {
        self.client
            .put(format!("{}/connectors/{}/resume", self.address, name))
            .send()
            .await?;
        Ok(())
    }

    /// Stop a connector
    pub async fn stop_connector(&self, name: &str) -> Result<()> {
        self.client
            .put(format!("{}/connectors/{}/stop", self.address, name))
            .send()
            .await?;
        Ok(())
    }

    /// get offsets for a connector
    pub async fn connector_offsets<P, O>(&self, name: &str) -> Result<Vec<ConnectorOffset<P, O>>>
    where
        P: serde::de::DeserializeOwned,
        O: serde::de::DeserializeOwned,
    {
        #[derive(serde::Deserialize)]
        struct ConnectorOffsetsResponse<P, O> {
            pub offsets: Vec<ConnectorOffset<P, O>>,
        }
        let response: ConnectorOffsetsResponse<P, O> = self
            .client
            .get(format!("{}/connectors/{}/offsets", self.address, name))
            .send()
            .await?
            .json()
            .await?;
        Ok(response.offsets)
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
        let connect = Connect::new(&format!("http://{}", server.host_with_port()), None, None);
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
                kind: Some("source".into()),
            }),
            status: None,
        };
        let mut expected = HashMap::new();
        expected.insert("test".to_string(), connector);
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/connectors")
            .match_query(mockito::Matcher::AnyOf(vec![
                mockito::Matcher::Exact("expand=info".to_string()),
                mockito::Matcher::Exact("expand=status".to_string()),
            ]))
            .with_body(serde_json::to_string(&expected).unwrap())
            .with_status(200)
            .create_async()
            .await;
        let connect = Connect::new(&server.url(), None, None);
        let actual = connect.connectors(false, true).await.unwrap();
        assert_eq!(expected, actual);
        // if both expand_info and expand_status are false, we expect an error
        let actual = connect.connectors(false, false).await;
        assert!(actual.is_err())
    }
}
