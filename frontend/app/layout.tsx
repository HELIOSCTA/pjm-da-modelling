import type { Metadata } from "next";
import "./globals.css";

import { Sidebar } from "./_components/Sidebar";

export const metadata: Metadata = {
  title: "PJM DA Frontend",
  description: "PJM day-ahead market dashboards and model outputs.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body
        suppressHydrationWarning
        className="min-h-screen bg-[#0f1117] text-gray-100 antialiased"
      >
        <div className="flex min-h-screen">
          <Sidebar />
          <div className="flex-1 overflow-x-hidden">{children}</div>
        </div>
      </body>
    </html>
  );
}
