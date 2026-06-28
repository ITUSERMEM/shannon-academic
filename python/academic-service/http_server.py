"""FastAPI HTTP receiver entry point for Phase 1a Go Gateway integration."""
import time

from fastapi import HTTPException
from pydantic import BaseModel

from academic_session import SessionManager, PHASE_NAMES

sm = SessionManager()
app = sm._make_fastapi_app()


# ── OpenAI-compatible /v1/chat/completions ──────────────────────────


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str = "academic"
    messages: list[ChatMessage]
    stream: bool = False


@app.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionRequest):
    # Extract title from the last user message
    title = "Academic Project"
    for msg in reversed(req.messages):
        if msg.role == "user" and msg.content.strip():
            title = msg.content.strip()[:100]
            break

    try:
        project_id = sm.start_project(title=title)
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))

    phases_summary = " → ".join(
        PHASE_NAMES.get(i, f"Phase {i}") for i in range(6)
    )

    return {
        "id": f"chatcmpl-{project_id}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": req.model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": (
                        f"Pipeline started. Session: {project_id}, "
                        f"Title: {title}. "
                        f"Phases: {phases_summary}"
                    ),
                },
                "finish_reason": "stop",
            }
        ],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
