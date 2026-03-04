import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useWebSocket } from "@/hooks/useWebSocket";
import Overview from "@/pages/Overview";
import Suggestions from "@/pages/Suggestions";
import History from "@/pages/History";
import Queries from "@/pages/Queries";
import Settings from "@/pages/Settings";
import { cn } from "@/lib/utils";
import {
  LayoutDashboard,
  Lightbulb,
  History as HistoryIcon,
  Search,
  SettingsIcon,
  Database,
} from "lucide-react";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 5_000,
    },
  },
});

const NAV_ITEMS = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/suggestions", label: "Suggestions", icon: Lightbulb },
  { to: "/history", label: "History", icon: HistoryIcon },
  { to: "/queries", label: "Queries", icon: Search },
  { to: "/settings", label: "Settings", icon: SettingsIcon },
] as const;

function Shell() {
  useWebSocket();

  return (
    <div className="flex h-screen">
      {/* Sidebar */}
      <aside className="w-56 shrink-0 border-r border-border bg-card flex flex-col">
        <div className="p-4 border-b border-border">
          <div className="flex items-center gap-2">
            <Database size={20} className="text-primary" />
            <span className="font-bold text-lg">pgAgent</span>
          </div>
          <p className="text-xs text-muted-foreground mt-1">
            PostgreSQL Monitor
          </p>
        </div>

        <nav className="flex-1 p-2 space-y-0.5">
          {NAV_ITEMS.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === "/"}
              className={({ isActive }) =>
                cn(
                  "flex items-center gap-2.5 px-3 py-2 rounded-md text-sm font-medium transition-colors",
                  isActive
                    ? "bg-primary/10 text-primary"
                    : "text-muted-foreground hover:text-foreground hover:bg-secondary/50"
                )
              }
            >
              <item.icon size={16} />
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className="p-4 border-t border-border text-xs text-muted-foreground">
          pgAgent v0.1.0
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-auto p-6">
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/suggestions" element={<Suggestions />} />
          <Route path="/history" element={<History />} />
          <Route path="/queries" element={<Queries />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </main>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Shell />
      </BrowserRouter>
    </QueryClientProvider>
  );
}
