"use client";

import { useEffect, useState } from "react";
import { checkBudget } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export function BudgetPanel() {
  const [budget, setBudget] = useState<{ allowed: boolean; remaining_budget: number } | null>(null);

  useEffect(() => {
    const load = async () => {
      const data = await checkBudget();
      setBudget(data);
    };
    load();
    const interval = setInterval(load, 30000);
    return () => clearInterval(interval);
  }, []);

  const pct = budget ? Math.round((budget.remaining_budget / 100) * 100) : 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Budget</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {!budget ? (
          <p className="text-sm text-muted-foreground">Loading...</p>
        ) : (
          <>
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">Remaining</span>
              <span className="font-mono font-medium">${budget.remaining_budget.toFixed(2)}</span>
            </div>
            <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-primary transition-all"
                style={{ width: `${pct}%` }}
              />
            </div>
            <p className="text-xs text-muted-foreground">
              {budget.allowed ? "Budget active" : "Budget exceeded"}
            </p>
          </>
        )}
      </CardContent>
    </Card>
  );
}
