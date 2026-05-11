import Link from "next/link";

export default function Home() {
  return (
    <main className="px-8 py-10">
      <h1 className="text-xl font-semibold tracking-tight">PJM DA — Operator Console</h1>
      <p className="mt-2 text-sm text-gray-400">
        Day-ahead market forecasts and model outputs.
      </p>
      <div className="mt-8 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        <Link
          href="/forecast"
          className="block rounded border border-gray-800 bg-gray-900/40 p-5 transition hover:border-gray-700 hover:bg-gray-900/70"
        >
          <p className="text-sm font-semibold text-gray-100">DA Forecast</p>
          <p className="mt-1 text-xs text-gray-500">
            KNN like-day point forecast with P10–P90 quantile bands, the
            ICE-anchored Meteo forecast, and a model-vs-model comparison for
            tomorrow&apos;s clear.
          </p>
        </Link>
      </div>
    </main>
  );
}
