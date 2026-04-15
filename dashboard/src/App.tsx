import { Route, Routes } from "react-router-dom";

import Navbar from "@/components/Navbar";
import Dashboard from "@/pages/Dashboard";
import Monitoring from "@/pages/Monitoring";

export default function App() {
  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <Navbar />
      <main className="mx-auto max-w-7xl px-4 py-6">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/monitoring" element={<Monitoring />} />
        </Routes>
      </main>
    </div>
  );
}
