"""Budget HTTP client — calls Go Gateway /budget/check for single-point budget authority."""

import os
import requests
from typing import Optional

GATEWAY_BASE = os.environ.get("GATEWAY_URL", "http://localhost:8080")


class BudgetHTTPClient:
    def __init__(self, base_url: str = ""):
        self.base_url = (base_url or GATEWAY_BASE).rstrip("/")

    def check_budget(
        self,
        user_id: str = "",
        model: str = "",
        estimated_cost: float = 0.0,
    ) -> dict:
        """Call Go Gateway budget check. Returns {"allowed": bool, "remaining_budget": float}."""
        resp = requests.post(
            f"{self.base_url}/api/v1/budget/check",
            json={"user_id": user_id, "model": model, "estimated_cost": estimated_cost},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
