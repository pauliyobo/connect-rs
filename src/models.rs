//! Connect rest interface models
//! Every struct defined here is used to interact with the kafka-connect API
//! The structures follow as of now the specification for kafka-connect  version 7.5
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ClusterInfo
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub version: String,
    pub commit: String,
    pub kafka_cluster_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Connector {
    pub info: Option<ConnectorInfo>,
    pub status: Option<ConnectorStatus>,
}

impl Connector {
    pub fn name(&self) -> &str {
        if self.info.is_some() {
            return &self.info.as_ref().unwrap().name;
        }
        &self.status.as_ref().unwrap().name
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorInfo {
    pub name: String,
    pub config: HashMap<String, String>,
    pub tasks: Vec<TaskInfo>,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskInfo {
    pub connector: String,
    pub task: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorStatus {
    pub connector: ConnectorState,
    pub name: String,
    pub tasks: Vec<TaskStatus>,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectorState {
    pub connector: Option<String>,
    pub state: Status,
    pub worker_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskStatus {
    pub id: u64,
    pub state: Status,
    pub worker_id: String,
    pub trace: Option<String>,
}

/// Status that a task or connector may be in
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
    Paused,
    Running,
    Restarting,
    Failed,
    Unassigned,
}
