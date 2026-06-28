"use client";

import { useEffect, useState } from "react";
import { fetchPipelineStatus } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

const STATUS_BADGE: Record<string, "default" | "success" | "warning" | "destructive"> = {
  running: "success",
  pending: "warning",
  failed: "destructive",
  offline: "destructive",
};

export function PipelinePanel() {
  const [pipelines, setPipelines] = useState<
    { id: string; name: string; status: string }[]
  >([]);

  useEffect(() => {
    const load = async () => {
      const data = await fetchPipelineStatus();
      setPipelines(data);
    };
    load();
    const interval = setInterval(load, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pipeline Status</CardTitle>
      </CardHeader>
      <CardContent>
        {pipelines.length === 0 ? (
          <p className="text-sm text-muted-foreground">Loading...</p>
        ) : (
          <ul className="space-y-2">
            {pipelines.map((p) => (
              <li key={p.id} className="flex items-center justify-between rounded-md border px-3 py-2 text-sm">
                <span>{p.name}</span>
                <Badge variant={STATUS_BADGE[p.status] ?? "default"}>{p.status}</Badge>
              </li>
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}
