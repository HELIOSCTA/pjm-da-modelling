// "Actuals released" / "Actuals pending" banner — the standard
// hand-off cue between forecast-bands view and post-clear evaluation
// view. Drop into any forecast tab where the payload exposes whether
// DA LMP has cleared for the target_date.

export function ActualsStatusBanner({
  released,
  targetDate,
}: {
  released: boolean;
  targetDate: string;
}) {
  if (released) {
    return (
      <section className="mb-6">
        <div className="flex items-center gap-3 rounded border border-emerald-700/60 bg-emerald-900/20 px-4 py-3">
          <span className="inline-block h-2.5 w-2.5 rounded-full bg-emerald-400 shadow-[0_0_6px_#34d399]" />
          <div>
            <div className="text-sm font-semibold text-emerald-200">
              Actuals released
            </div>
            <div className="text-xs text-emerald-100/70">
              DA LMPs cleared for {targetDate} · evaluation tables below.
            </div>
          </div>
        </div>
      </section>
    );
  }
  return (
    <section className="mb-6">
      <div className="flex items-center gap-3 rounded border border-gray-700 bg-gray-900/40 px-4 py-3">
        <span className="inline-block h-2.5 w-2.5 rounded-full bg-gray-500" />
        <div>
          <div className="text-sm font-semibold text-gray-300">
            Actuals pending
          </div>
          <div className="text-xs text-gray-400">
            Awaiting DA clear for {targetDate}. Forecast bands shown below;
            evaluation tables appear once actuals land.
          </div>
        </div>
      </div>
    </section>
  );
}
