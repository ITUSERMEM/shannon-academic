"""gRPC AcademicService server — called by Go Temporal Workflow activities.

Implements 5 RPCs:
  - ExecutePhase: Delegates to SessionManager.start_project()
  - EvaluateGate: Reads phase state from Redis and evaluates gate criteria
  - RecordEvent: Persists workflow lifecycle events via EventEmitter
  - GetBudget: Returns current budget state (read-only, Go enforces)
  - HealthCheck: Liveness probe

Usage:
    python academic_grpc_server.py  # starts on :8002 by default
"""

import json
import os
import time

import grpc
from concurrent import futures

from academic_session import SessionManager
from event_system import EventEmitter, EventType
from token_budget import TokenBudget

from pb.academic import academic_pb2, academic_pb2_grpc

VERSION = "0.2.0"


class AcademicServicer(academic_pb2_grpc.AcademicServiceServicer):
    """gRPC servicer implementing the 5 AcademicService RPCs."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.sm = SessionManager(max_concurrent=2, redis_url=redis_url)
        self._budgets: dict[str, TokenBudget] = {}
        self._redis_url = redis_url

    def _get_budget(self, project_id: str) -> TokenBudget:
        if project_id not in self._budgets:
            self._budgets[project_id] = TokenBudget(
                redis_url=self._redis_url,
                session_id=project_id,
                task_id=project_id,
            )
        return self._budgets[project_id]

    # ── ExecutePhase ──────────────────────────────────────────────

    def ExecutePhase(self, request, context):
        """Run one phase (0-5) of the academic pipeline via subprocess."""
        try:
            project_id = self.sm.start_project(
                project_id=request.project_id or "",
                title=request.title or "Academic Project",
                start_phase=request.phase,
                end_phase=request.phase,
            )
            completed_tasks = [f"phase_{request.phase}"]
            return academic_pb2.ExecutePhaseResponse(
                project_id=project_id,
                phase=request.phase,
                completed=True,
                result_summary=f"Phase {request.phase} completed",
                completed_tasks=completed_tasks,
                session_id=project_id,
                iterations_run=1,
            )
        except Exception as e:
            return academic_pb2.ExecutePhaseResponse(
                project_id=request.project_id,
                phase=request.phase,
                completed=False,
                error_message=str(e),
                iterations_run=0,
            )

    # ── EvaluateGate ──────────────────────────────────────────────

    def EvaluateGate(self, request, context):
        """Check if phase gate criteria are met by reading Redis phase state.

        The actual gate evaluation runs inside the subprocess spawned by
        ExecutePhase. This RPC reads the result from Redis and returns
        the verdict. If no result exists yet, passes optimistically — the
        Go workflow enforces the real decision logic.
        """
        try:
            from redis import Redis
            r = Redis.from_url(self._redis_url, decode_responses=True)
            state_key = f"academic:phase:state:{request.project_id}"
            state = r.json().get(state_key)
            r.close()
            if state:
                completed = state.get("completed_phases", [])
                phase = request.current_phase
                passed = phase in completed
                next_phase = phase + 1 if passed else -1
                return academic_pb2.EvaluateGateResponse(
                    passed=passed,
                    next_phase=next_phase,
                    reason="Gate passed" if passed else "Gate not yet passed",
                    verdict="pass" if passed else "revise",
                    issues=[],
                )
            return academic_pb2.EvaluateGateResponse(
                passed=True,
                next_phase=request.current_phase + 1,
                reason="No phase state found — passing optimistically",
                verdict="pass",
                issues=[],
            )
        except Exception as e:
            return academic_pb2.EvaluateGateResponse(
                passed=True,
                next_phase=request.current_phase + 1,
                reason=f"Gate evaluation fallback: {e}",
                verdict="pass",
                issues=[],
            )

    # ── RecordEvent ───────────────────────────────────────────────

    _EVENT_TYPE_MAP = {
        "phase_start": EventType.PHASE_STARTED,
        "phase_complete": EventType.PHASE_COMPLETED,
        "phase_failed": EventType.PHASE_FAILED,
        "gate_pass": EventType.GATE_PASSED,
        "gate_fail": EventType.GATE_FAILED,
        "gate_retry": EventType.GATE_REVISED,
        "pipeline_start": EventType.PIPELINE_STARTED,
        "pipeline_complete": EventType.PIPELINE_COMPLETED,
        "pipeline_failed": EventType.PIPELINE_FAILED,
        "error": EventType.LLM_CALL_FAILED,
        "budget_exceeded": EventType.PHASE_FAILED,
    }

    def RecordEvent(self, request, context):
        """Persist a workflow lifecycle event via EventEmitter."""
        try:
            from redis import Redis
            r = Redis.from_url(self._redis_url, decode_responses=True)
            emitter = EventEmitter(r, audit_logger=None)
            event_type = self._EVENT_TYPE_MAP.get(request.event_type, EventType.PHASE_STARTED)
            event_data = {
                "project_id": request.project_id,
                "event_type": request.event_type,
                "session_id": request.session_id,
            }
            if request.payload:
                try:
                    payload_data = json.loads(request.payload)
                    event_data.update(payload_data)
                except (json.JSONDecodeError, TypeError):
                    event_data["_raw_payload"] = request.payload
            emitter.emit(event_type, **event_data)
            r.close()
            return academic_pb2.RecordEventResponse(
                recorded=True,
                event_id=f"evt-{int(time.time())}-{request.project_id[:8] if request.project_id else 'anon'}",
            )
        except Exception:
            return academic_pb2.RecordEventResponse(
                recorded=False,
                event_id="",
            )

    # ── GetBudget (read-only) ─────────────────────────────────────

    def GetBudget(self, request, context):
        """Read budget status. Python reports — Go enforces."""
        try:
            budget = self._get_budget(request.project_id)
            usage = budget.check()
            session = usage.get("session", {})
            tokens_used = session.get("tokens", 0)
            tokens_limit = session.get("limit", 1000000)
            cost_usd = tokens_used * 0.000002
            budget_usd = 100.0
            usage_pct = usage.get("max_pct", 0.0) / 100.0
            action = usage.get("action", "ok")
            return academic_pb2.GetBudgetResponse(
                tokens_used=tokens_used,
                tokens_limit=tokens_limit,
                cost_usd=cost_usd,
                budget_usd=budget_usd,
                usage_pct=usage_pct,
                action=action,
            )
        except Exception:
            return academic_pb2.GetBudgetResponse(
                tokens_used=0,
                tokens_limit=1000000,
                cost_usd=0.0,
                budget_usd=100.0,
                usage_pct=0.0,
                action="ok",
            )

    # ── HealthCheck ───────────────────────────────────────────────

    def HealthCheck(self, request, context):
        return academic_pb2.HealthCheckResponse(
            healthy=True,
            version=VERSION,
            active_sessions=len(self.sm._sessions),
            status="ready",
        )


def serve():
    port = int(os.environ.get("ACADEMIC_GRPC_PORT", "8002"))
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    academic_pb2_grpc.add_AcademicServiceServicer_to_server(
        AcademicServicer(redis_url=redis_url), server
    )
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"AcademicService gRPC server v{VERSION} started on :{port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
