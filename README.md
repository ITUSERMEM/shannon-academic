# shannon-academic

> Academic research pipeline — Go Gateway + Rust enforcement + Python LLM service + Temporal orchestration.

[![CI](https://github.com/ITUSERMEM/shannon-academic/actions/workflows/ci.yml/badge.svg)](https://github.com/ITUSERMEM/shannon-academic/actions/workflows/ci.yml)

## Architecture

### Three-Layer Pipeline

```mermaid
graph TB
    Client["Client (OpenAI SDK / curl)"] --> Gateway["Go Gateway :8080"]
    
    subgraph Gateway["Go Gateway"]
        OpenAI["OpenAI /v1/chat/completions"]
        Budget["/api/v1/budget/check"]
        Workflow["/api/v1/workflow/start"]
        CB["/api/v1/circuitbreaker/status"]
    end
    
    Gateway -->|"gRPC :50051"| Rust["Rust Agent-Core<br/>enforcement + sandbox"]
    Gateway -->|"gRPC :8002"| Python["Python AcademicService<br/>ExecutePhase / EvaluateGate"]
    Gateway -->|"HTTP :8001"| PythonHTTP["Python HTTP Server<br/>/v1/chat/completions"]
    
    Python --> Temporal["Temporal Workflow<br/>Phase 0-5 Loop"]
    Temporal --> Redis[("Redis Stack<br/>:6379")]
    Temporal --> PG[("PostgreSQL<br/>:5432")]
```

### Phase Execution Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway :8080
    participant T as Temporal
    participant P as Python gRPC :8002
    
    C->>G: POST /v1/workflow/start
    G->>T: StartWorkflow(AcademicWorkflow)
    T->>P: ExecutePhase(phase=0)
    P-->>T: completed=True
    T->>P: EvaluateGate(phase=0)
    P-->>T: passed=True, nextPhase=1
    T->>P: ExecutePhase(phase=1)
    P-->>T: completed=True
    T-->>G: WorkflowCompleted
    G-->>C: {"run_id":"...","workflow_id":"..."}
```

### Deployment

```mermaid
graph LR
    subgraph Docker
        Redis["redis-stack:6379"]
        PG["postgres:5432"]
        Temporal["temporal:7233"]
        UITemporal["temporal-ui:8088"]
    end
    
    subgraph Systemd
        PythonSvc["academic-python.service<br/>:8002 gRPC"]
        GatewaySvc["academic-gateway.service<br/>:8080 HTTP"]
    end
    
    Docker --> Systemd
```

## Quick Start

### Docker Compose (recommended)

```bash
git clone https://github.com/ITUSERMEM/shannon-academic.git
cd shannon-academic
docker compose -f deploy/academic/docker-compose.yml up -d
curl http://localhost:8080/v1/health
open http://localhost:8080/docs
```

### systemd (local dev)

```bash
sudo cp deploy/academic/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start academic-python
sudo systemctl start academic-gateway
journalctl -u academic-gateway -f
```

### OpenAI-Compatible API

```python
import openai
client = openai.OpenAI(base_url="http://localhost:8080/v1", api_key="dev")
resp = client.chat.completions.create(
    model="academic",
    messages=[{"role": "user", "content": "Fault diagnosis with deep learning"}]
)
print(resp.choices[0].message.content)
# → "Pipeline started. Session: abc123, Phases: Phase 0 → Phase 1 → ..."
```

## Architecture

| Layer | Language | Port | Role |
|-------|----------|------|------|
| **Gateway** | Go | `:8080` | OpenAI API, budget, circuit breaker, Temporal |
| **Agent-Core** | Rust | `:50051` | Rate limit, sandbox, entropy monitor |
| **AcademicService** | Python | `:8002` gRPC, `:8001` HTTP | Phase execution, gate evaluation, events |
| **Orchestration** | Temporal | `:7233` | Workflow state machine, retry, replay |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/health` | Health check |
| GET | `/v1/models` | List models |
| POST | `/v1/chat/completions` | OpenAI-compatible |
| POST | `/api/v1/workflow/start` | Start pipeline |
| POST | `/api/v1/budget/check` | Budget check |
| GET | `/api/v1/circuitbreaker/status` | Circuit breaker |
| GET | `/api/v1/degradation/level` | Degradation status |

Full API docs: [http://localhost:8080/docs](http://localhost:8080/docs)

## Project Structure

```
shannon-academic/
├── go/orchestrator/         # Go Gateway + Temporal workflow
├── rust/agent-core/         # Rust enforcement + sandbox
├── python/academic-service/ # Python gRPC + HTTP server
├── protos/                  # gRPC protocol buffers
├── config/                  # Model registry, rate limits
├── deploy/                  # Docker, systemd, env
├── api/                     # OpenAPI specification
├── migrations/              # PostgreSQL schema
└── docs/                    # (coming soon)
```

## License

Apache 2.0
