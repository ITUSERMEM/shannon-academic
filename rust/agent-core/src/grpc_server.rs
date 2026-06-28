use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::enforcement::RequestEnforcer;

// Include the generated proto code
#[allow(clippy::enum_variant_names)]
pub mod proto {
    pub mod agent {
        tonic::include_proto!("shannon.agent");
    }
    #[allow(clippy::enum_variant_names)]
    pub mod common {
        tonic::include_proto!("shannon.common");
    }

    // Export file descriptor for reflection
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("shannon_descriptor");
}

use proto::agent::agent_service_server::{AgentService, AgentServiceServer};
use proto::agent::*;

// Streaming limits to prevent resource exhaustion
const MAX_STREAM_BUFFER_SIZE: usize = 1_000_000; // 1MB max buffer size
const DEFAULT_STREAM_TIMEOUT_SECS: u64 = 600; // 10 minutes default timeout

pub struct AgentServiceImpl {
    start_time: std::time::Instant,
    enforcer: std::sync::Arc<RequestEnforcer>,
}

impl Default for AgentServiceImpl {
    fn default() -> Self {
        Self::new().expect("Failed to create AgentServiceImpl in Default trait")
    }
}

impl AgentServiceImpl {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            start_time: std::time::Instant::now(),
            enforcer: std::sync::Arc::new(RequestEnforcer::from_global()?),
        })
    }

    pub fn into_service(self) -> AgentServiceServer<Self> {
        AgentServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl AgentService for AgentServiceImpl {
    async fn execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        let req = request.into_inner();
        info!("Executing task (delegated to Python backend): {}", req.query);

        // Python backend handles all LLM/tool execution.
        // This Rust gateway provides enforcement and metrics.

        let response = ExecuteTaskResponse {
            task_id: req
                .metadata
                .as_ref()
                .map(|m| m.task_id.clone())
                .unwrap_or_default(),
            status: proto::common::StatusCode::Ok.into(),
            result: String::new(),
            tool_calls: Vec::new(),
            tool_results: Vec::new(),
            metrics: Some(proto::common::ExecutionMetrics {
                latency_ms: 0,
                token_usage: None,
                cache_hit: false,
                cache_score: 0.0,
                agents_used: 0,
                mode: req.mode,
            }),
            error_message: String::new(),
            final_state: proto::agent::AgentState::Completed.into(),
            metadata: None,
        };

        Ok(Response::new(response))
    }

    type StreamExecuteTaskStream =
        tokio_stream::wrappers::ReceiverStream<Result<TaskUpdate, Status>>;

    async fn stream_execute_task(
        &self,
        request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<Self::StreamExecuteTaskStream>, Status> {
        let req = request.into_inner();
        info!("Stream executing task (delegated to Python backend): {}", req.query);

        let (tx, rx) = mpsc::channel(128);
        let task_id = req
            .metadata
            .as_ref()
            .map(|m| m.task_id.clone())
            .unwrap_or_else(|| "stream-task".to_string());

        tokio::spawn(async move {
            let _ = tx
                .send(Ok(TaskUpdate {
                    task_id: task_id.clone(),
                    state: proto::agent::AgentState::Completed.into(),
                    message: "Execution delegated to Python backend".to_string(),
                    tool_call: None,
                    tool_result: None,
                    progress: 1.0,
                    delta: String::new(),
                }))
                .await;
        });

        Ok(Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx) as Self::StreamExecuteTaskStream,
        ))
    }

    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        debug!("Getting agent capabilities");

        let response = GetCapabilitiesResponse {
            supported_tools: {
                vec![
                    "web_search".to_string(),
                    "database_query".to_string(),
                ]
            },
            supported_modes: vec![
                proto::common::ExecutionMode::Simple.into(),
                proto::common::ExecutionMode::Standard.into(),
                proto::common::ExecutionMode::Complex.into(),
            ],
            max_memory_mb: 512,
            max_concurrent_tasks: 10,
            version: env!("CARGO_PKG_VERSION").to_string(),
        };

        Ok(Response::new(response))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        debug!("Health check requested");

        let response = HealthCheckResponse {
            healthy: true,
            message: "Agent core is healthy".to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs() as i64,
            active_tasks: 0,
            memory_usage_percent: 0.0,
        };

        Ok(Response::new(response))
    }

    async fn discover_tools(
        &self,
        _request: Request<DiscoverToolsRequest>,
    ) -> Result<Response<DiscoverToolsResponse>, Status> {
        debug!("Tool discovery requested");

        let response = DiscoverToolsResponse {
            tools: vec![], // Stub implementation
        };

        Ok(Response::new(response))
    }

    async fn get_tool_capability(
        &self,
        _request: Request<GetToolCapabilityRequest>,
    ) -> Result<Response<GetToolCapabilityResponse>, Status> {
        debug!("Tool capability requested");

        let response = GetToolCapabilityResponse {
            tool: None, // Stub implementation
        };

        Ok(Response::new(response))
    }
}

// Helper: convert prost_types::Value to serde_json::Value for passing context to Python
fn tool_meta_to_proto(
    meta: &Option<serde_json::Value>,
) -> (Vec<proto::common::ToolCall>, Vec<proto::common::ToolResult>) {
    if let Some(meta_val) = meta {
        if let Some(exec_list) = meta_val.get("tool_executions").and_then(|v| v.as_array()) {
            let mut calls = Vec::new();
            let mut results = Vec::new();
            for exec in exec_list {
                let tool_name = exec
                    .get("tool")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if tool_name.is_empty() {
                    continue;
                }
                let output_val = exec
                    .get("output")
                    .map(crate::grpc_server::prost_value_to_json_to_prost);
                let status = if exec
                    .get("success")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    proto::common::StatusCode::Ok.into()
                } else {
                    proto::common::StatusCode::Error.into()
                };
                let err_msg = exec
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let duration_ms = exec
                    .get("duration_ms")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                calls.push(proto::common::ToolCall {
                    name: tool_name.clone(),
                    parameters: None,
                    tool_id: tool_name.clone(),
                });
                results.push(proto::common::ToolResult {
                    tool_id: tool_name,
                    output: output_val,
                    status,
                    error_message: err_msg,
                    execution_time_ms: duration_ms,
                });
            }
            return (calls, results);
        }
    }
    (Vec::new(), Vec::new())
}

pub fn prost_value_to_json(v: &prost_types::Value) -> serde_json::Value {
    use prost_types::value::Kind::*;
    match v.kind.as_ref() {
        Some(NullValue(_)) => serde_json::Value::Null,
        Some(BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(NumberValue(n)) => serde_json::json!(*n),
        Some(StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(ListValue(lv)) => {
            serde_json::Value::Array(lv.values.iter().map(prost_value_to_json).collect())
        }
        Some(StructValue(st)) => {
            let mut map = serde_json::Map::new();
            for (k, v) in &st.fields {
                map.insert(k.clone(), prost_value_to_json(v));
            }
            serde_json::Value::Object(map)
        }
        None => serde_json::Value::Null,
    }
}

// Helper: convert serde_json::Value back to prost_types::Value
fn prost_value_to_json_to_prost(v: &serde_json::Value) -> prost_types::Value {
    use prost_types::value::Kind;
    match v {
        serde_json::Value::Null => prost_types::Value {
            kind: Some(Kind::NullValue(0)),
        },
        serde_json::Value::Bool(b) => prost_types::Value {
            kind: Some(Kind::BoolValue(*b)),
        },
        serde_json::Value::Number(n) => prost_types::Value {
            kind: Some(Kind::NumberValue(n.as_f64().unwrap_or(0.0))),
        },
        serde_json::Value::String(s) => prost_types::Value {
            kind: Some(Kind::StringValue(s.clone())),
        },
        serde_json::Value::Array(arr) => prost_types::Value {
            kind: Some(Kind::ListValue(prost_types::ListValue {
                values: arr.iter().map(prost_value_to_json_to_prost).collect(),
            })),
        },
        serde_json::Value::Object(map) => prost_types::Value {
            kind: Some(Kind::StructValue(prost_types::Struct {
                fields: map
                    .iter()
                    .map(|(k, v)| (k.clone(), prost_value_to_json_to_prost(v)))
                    .collect(),
            })),
        },
    }
}

/// Convert `Option<serde_json::Value>` (expected Object) to `Option<prost_types::Struct>`.
/// Used to forward agent metadata (including tool_cost_entries) through gRPC responses.
fn serde_json_to_prost_struct(meta: &Option<serde_json::Value>) -> Option<prost_types::Struct> {
    match meta {
        Some(serde_json::Value::Object(map)) => Some(prost_types::Struct {
            fields: map
                .iter()
                .map(|(k, v)| (k.clone(), prost_value_to_json_to_prost(v)))
                .collect(),
        }),
        Some(other) => {
            tracing::debug!(
                "serde_json_to_prost_struct: metadata is not an object (type={:?}), skipping",
                other
            );
            None
        }
        None => None,
    }
}

// Helper: produce a simple, user-facing string from a serde_json::Value.
fn extract_simple_text_from_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(_) => v.to_string(),
        serde_json::Value::Object(map) => {
            if let Some(inner) = map.get("result") {
                return extract_simple_text_from_json(inner);
            }
            v.to_string()
        }
    }
}

// Helper: same as above, but starting from prost_types::Value
fn extract_simple_text_from_prost(v: &prost_types::Value) -> String {
    let json = prost_value_to_json(v);
    extract_simple_text_from_json(&json)
}
