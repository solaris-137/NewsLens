import { Link, useLocation } from "react-router-dom";

export default function Navbar() {
  const { pathname } = useLocation();

  return (
    <nav className="border-b border-slate-200/80 bg-white/90 px-4 py-3 backdrop-blur">
      <div className="mx-auto flex max-w-7xl items-center justify-between">
        <span className="font-semibold tracking-tight text-slate-900">
          Apple Sentiment
        </span>
        <div className="flex gap-6 text-sm">
          <Link
            to="/"
            className={
              pathname === "/"
                ? "font-medium text-blue-600"
                : "text-slate-500 transition-colors hover:text-slate-800"
            }
          >
            Dashboard
          </Link>
          <Link
            to="/monitoring"
            className={
              pathname === "/monitoring"
                ? "font-medium text-blue-600"
                : "text-slate-500 transition-colors hover:text-slate-800"
            }
          >
            Monitoring
          </Link>
        </div>
      </div>
    </nav>
  );
}
