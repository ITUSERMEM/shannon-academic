"use client";

import { createContext, useContext, useState, useCallback, type ComponentProps } from "react";
import { cn } from "@/lib/utils";
import { Slot } from "@radix-ui/react-slot";

/* ---------- context ---------- */
type SidebarContext = { open: boolean; setOpen: (v: boolean) => void };
const SidebarCtx = createContext<SidebarContext>({ open: true, setOpen: () => {} });

export function SidebarProvider({ children, ...props }: { children: React.ReactNode; defaultOpen?: boolean; open?: boolean; onOpenChange?: (v: boolean) => void }) {
  const [internalOpen, setInternalOpen] = useState(props.defaultOpen ?? true);
  const open = props.open ?? internalOpen;
  const setOpen = useCallback((v: boolean) => {
    setInternalOpen(v);
    props.onOpenChange?.(v);
  }, [props.onOpenChange]);

  return (
    <SidebarCtx.Provider value={{ open, setOpen }}>
      {children}
    </SidebarCtx.Provider>
  );
}

export function SidebarTrigger({ className, ...props }: ComponentProps<"button">) {
  const ctx = useContext(SidebarCtx);
  return (
    <button
      data-sidebar-trigger
      className={cn("inline-flex items-center justify-center rounded-md p-1 hover:bg-muted", className)}
      onClick={() => ctx.setOpen(!ctx.open)}
      {...props}
    >
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
        <line x1="2" y1="3" x2="14" y2="3" />
        <line x1="2" y1="8" x2="14" y2="8" />
        <line x1="2" y1="13" x2="14" y2="13" />
      </svg>
    </button>
  );
}

/* ---------- sidebar shell ---------- */
export function Sidebar({ className, children, ...props }: ComponentProps<"div">) {
  const ctx = useContext(SidebarCtx);
  return (
    <aside
      data-sidebar
      data-state={ctx.open ? "expanded" : "collapsed"}
      className={cn(
        "flex flex-col border-r bg-sidebar text-sidebar-foreground transition-all duration-200",
        ctx.open ? "w-60" : "w-0 overflow-hidden md:w-14",
        className,
      )}
      {...props}
    >
      {children}
    </aside>
  );
}

export function SidebarHeader({ className, ...props }: ComponentProps<"div">) {
  return <div data-sidebar-header className={cn("flex items-center border-b px-4 py-2", className)} {...props} />;
}

export function SidebarContent({ className, ...props }: ComponentProps<"div">) {
  return <div data-sidebar-content className={cn("flex-1 overflow-y-auto p-2", className)} {...props} />;
}

export function SidebarGroup({ className, ...props }: ComponentProps<"div">) {
  return <div data-sidebar-group className={cn("", className)} {...props} />;
}

export function SidebarGroupLabel({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      data-sidebar-group-label
      className={cn("px-2 py-1 text-xs font-semibold text-muted-foreground uppercase tracking-wider", className)}
      {...props}
    />
  );
}

export function SidebarGroupContent({ className, ...props }: ComponentProps<"div">) {
  return <div data-sidebar-group-content className={cn("", className)} {...props} />;
}

export function SidebarMenu({ className, ...props }: ComponentProps<"div">) {
  return <div data-sidebar-menu className={cn("space-y-0.5", className)} {...props} />;
}

export function SidebarMenuItem({ className, ...props }: ComponentProps<"div">) {
  return <div data-sidebar-menu-item className={cn("", className)} {...props} />;
}

export function SidebarMenuButton({
  asChild,
  className,
  children,
  ...props
}: ComponentProps<"button"> & { asChild?: boolean }) {
  const Comp = asChild ? Slot : "button";
  const ctx = useContext(SidebarCtx);
  return (
    <Comp
      data-sidebar-menu-button
      data-active={false}
      className={cn(
        "flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-sidebar-primary/10 aria-disabled:opacity-50",
        !ctx.open && "md:justify-center",
        className,
      )}
      {...props}
    >
      {children}
    </Comp>
  );
}
