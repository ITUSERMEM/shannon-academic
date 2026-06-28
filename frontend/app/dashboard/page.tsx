"use client";

import { AppLayout } from "@/components/app-layout";
import { PipelinePanel } from "@/components/pipeline-panel";
import { BudgetPanel } from "@/components/budget-panel";
import { MetricsPanel } from "@/components/metrics-panel";

export default function DashboardPage() {
  return (
    <AppLayout>
      <div className="grid gap-6 p-6">
        <PipelinePanel />
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <BudgetPanel />
          <MetricsPanel />
        </div>
      </div>
    </AppLayout>
  );
}
