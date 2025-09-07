//! Connect rest interface models
//! Every struct defined here is used to interact with the kafka-connect API
//! The structures follow as of now the specification for kafka-connect  version 7.5
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ClusterInfo
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub version: String,
    pub commit: String,
    pub kafka_cluster_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectorInfo {
    pub name: String,
    pub config: HashMap<String, String>,
    pub tasks: Vec<TaskInfo>,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskInfo {
    pub connector: String,
    pub task: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectorStatus {
    pub connector: ConnectorState,
    pub name: String,
    pub tasks: Vec<TaskStatus>,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectorState {
    pub connector: Option<String>,
    pub state: Status,
    pub worker_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
    Stopped,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unassigned => write!(f, "Unassigned")?,
            Self::Failed => write!(f, "Failed")?,
            Self::Paused => write!(f, "Paused")?,
            Self::Restarting => write!(f, "Restarting")?,
            Self::Running => write!(f, "Running")?,
            Self::Stopped => write!(f, "Stopped")?,
        };
        Ok(())
    }
}

/// kafka source connector offset
/// Source connectors may represent partition and offset information in their own specific way
#[derive(Clone, Debug, Serialize, PartialEq, Eq, Deserialize)]
pub struct SourceConnectorOffset<P, O> {
    /// partition offset
    pub partition: P,
    // connector offset
    pub offset: O,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SinkConnectorOffsetPartition {
    pub kafka_topic: String,
    pub kafka_partition: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SinkConnectorOffsetOffset {
    pub offset: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SinkConnectorOffset {
    pub partition: SinkConnectorOffsetPartition,
    pub offset: SinkConnectorOffsetOffset,
}

/// sink connector offset representation
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ConnectorOffset<P, O> {
    Source(SourceConnectorOffset<P, O>),
    Sink(SinkConnectorOffset),
}

impl<'de, P, O> Deserialize<'de> for ConnectorOffset<P, O>
where
    P: Deserialize<'de>,
    O: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Inner<P, O> {
            Source(SourceConnectorOffset<P, O>),
            Sink(SinkConnectorOffset),
        }

        match Inner::<P, O>::deserialize(deserializer) {
            Ok(Inner::Source(source)) => Ok(ConnectorOffset::Source(source)),
            Ok(Inner::Sink(sink)) => Ok(ConnectorOffset::Sink(sink)),
            Err(e) => Err(serde::de::Error::custom(format!(
                "Failed to deserialize ConnectorOffset: {}",
                e
            ))),
        }
    }
}
