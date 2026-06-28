const GATEWAY = process.env.NEXT_PUBLIC_GATEWAY_URL || "http://localhost:8080";

export async function fetchAPI(path: string, options?: RequestInit) {
  const res = await fetch(`${GATEWAY}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) throw new Error(`API ${path}: ${res.status}`);
  return res.json();
}

export async function fetchHealth() {
  return fetchAPI("/v1/health");
}

export async function fetchModels() {
  return fetchAPI("/v1/models");
}

export async function fetchPipelineStatus() {
  try {
    const data = await fetchAPI("/v1/health");
    return [
      { id: "phase-0", name: "Phase 0: Resource Provisioning", status: data.status === "healthy" ? "running" : "failed" },
      { id: "phase-1", name: "Phase 1: Literature Search", status: "pending" },
      { id: "phase-2", name: "Phase 2: Experiment Design", status: "pending" },
    ];
  } catch {
    return [
      { id: "phase-0", name: "Phase 0: Resource Provisioning", status: "offline" },
      { id: "phase-1", name: "Phase 1: Literature Search", status: "offline" },
      { id: "phase-2", name: "Phase 2: Experiment Design", status: "offline" },
    ];
  }
}

export async function startWorkflow() {
  return fetchAPI("/api/v1/workflow/start", { method: "POST" });
}

export async function checkBudget() {
  try {
    return await fetchAPI("/api/v1/budget/check", { method: "POST" });
  } catch {
    return { allowed: true, remaining_budget: 100.0 };
  }
}

export async function fetchCircuitBreakerStatus() {
  try {
    return await fetchAPI("/api/v1/circuitbreaker/status");
  } catch {
    return { status: "closed", failure_count: 0 };
  }
}

export async function fetchDegradationLevel() {
  try {
    return await fetchAPI("/api/v1/degradation/level");
  } catch {
    return { level: 0, status: "normal" };
  }
}

export async function fetchModelsList() {
  try {
    const data = await fetchAPI("/v1/models");
    return data.data || [];
  } catch {
    return [{ id: "academic", status: "unreachable" }];
  }
}
