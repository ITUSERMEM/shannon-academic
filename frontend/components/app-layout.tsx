"use client";

import { AppSidebar } from "./app-sidebar";
import { SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";

export function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <SidebarProvider>
      <div className="flex h-screen w-full overflow-hidden bg-background">
        <AppSidebar />
        <main className="flex flex-1 flex-col overflow-y-auto">
          <div className="sticky top-0 z-10 flex shrink-0 items-center gap-2 border-b bg-background px-4 py-2">
            <SidebarTrigger className="cursor-pointer" />
            <span className="text-sm font-medium">shannon-academic</span>
          </div>
          <div className="flex-1">{children}</div>
        </main>
      </div>
    </SidebarProvider>
  );
}
