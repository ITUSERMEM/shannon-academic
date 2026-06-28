"use client";

import { useEffect, useState } from "react";
import { fetchCircuitBreakerStatus, fetchDegradationLevel, fetchModelsList, fetchHealth } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

type Metric = { label: string; value: string; sub: string };

export function MetricsPanel() {
  const [metrics, setMetrics] = useState<Metric[]>([
    { label: "Circuit Breaker", value: "—", sub: "closed" },
    { label: "Degradation", value: "—", sub: "normal" },
    { label: "Models", value: "—", sub: "0 online" },
    { label: "Agents", value: "—", sub: "0 active" },
  ]);

  useEffect(() => {
    const load = async () => {
      const [cb, deg, models] = await Promise.all([
        fetchCircuitBreakerStatus().catch(() => ({ status: "closed", failure_count: 0 })),
        fetchDegradationLevel().catch(() => ({ level: 0, status: "normal" })),
        fetchModelsList().catch(() => []),
      ]);
      setMetrics([
        { label: "Circuit Breaker", value: cb.status, sub: `${cb.failure_count} failures` },
        { label: "Degradation", value: `Level ${deg.level}`, sub: deg.status },
        { label: "Models", value: `${models.length}`, sub: `${models.filter((m: any) => m.status !== "unreachable").length} online` },
        { label: "Agents", value: "4", sub: "idle" },
      ]);
    };
    load();
    const interval = setInterval(load, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="grid grid-cols-2 gap-3">
      {metrics.map((m) => (
        <Card key={m.label}>
          <CardHeader>
            <CardTitle>{m.label}</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-xl font-bold">{m.value}</p>
            <p className="text-xs text-muted-foreground">{m.sub}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
