import { useCallback, useEffect, useMemo, useState } from "react";
import { ApiError, getAnalytics, getRejections, getRuns, listDatasets } from "./api";
import type { Dataset, HourlyZoneStat, RejectionRecord, ReplayRun } from "./types";

const JAN_START = "2024-01-01T00:00";
const FEB_START = "2024-02-01T00:00";

function toUtc(value: string) {
  return `${value}:00Z`;
}

function compactNumber(value: number) {
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function formatMoney(cents: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(cents / 100);
}

function formatBytes(value: number) {
  if (value < 1024) return `${value} B`;
  const units = ["KB", "MB", "GB"];
  let size = value / 1024;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[index]}`;
}

function formatUtc(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    hour12: false,
    timeZone: "UTC",
  }).format(new Date(value));
}

function errorMessage(error: unknown) {
  if (error instanceof ApiError) {
    return error.requestId ? `${error.message} · request ${error.requestId}` : error.message;
  }
  return error instanceof Error ? error.message : "The dashboard could not load this view.";
}

function Mark() {
  return (
    <svg aria-hidden="true" className="brand-mark" viewBox="0 0 38 38">
      <path d="M5 26.5 12.2 10h6.1L11 26.5H5Z" />
      <path d="M15.3 26.5 22.5 10h10.2l-2.6 5.9h-4.2l-1.4 3.2h4.2l-3.2 7.4H15.3Z" />
    </svg>
  );
}

function TrendChart({ rows }: { rows: HourlyZoneStat[] }) {
  const points = useMemo(() => {
    const byHour = new Map<number, number>();
    for (const row of rows) {
      const key = new Date(row.pickup_hour_utc).getTime();
      byHour.set(key, (byHour.get(key) ?? 0) + row.trip_count);
    }
    return [...byHour].sort((a, b) => a[0] - b[0]);
  }, [rows]);

  if (points.length === 0) {
    return <div className="empty-chart">No accepted trips in this range</div>;
  }

  const width = 900;
  const height = 250;
  const max = Math.max(...points.map(([, value]) => value), 1);
  const line = points
    .map(([, value], index) => {
      const x = points.length === 1 ? width / 2 : (index / (points.length - 1)) * width;
      const y = height - (value / max) * (height - 28) - 8;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <div className="chart-wrap">
      <svg aria-label="Accepted trips by hour" className="trend-chart" role="img" viewBox={`0 0 ${width} ${height}`}>
        <line x1="0" x2={width} y1="242" y2="242" />
        <line x1="0" x2={width} y1="125" y2="125" />
        <line x1="0" x2={width} y1="8" y2="8" />
        <polyline points={line} />
      </svg>
      <div className="chart-axis">
        <span>{formatUtc(new Date(points[0][0]).toISOString())}</span>
        <span>UTC</span>
        <span>{formatUtc(new Date(points.at(-1)![0]).toISOString())}</span>
      </div>
    </div>
  );
}

function StatusPill({ dataset }: { dataset: Dataset }) {
  const state = dataset.latest_run_state ?? "NOT RUN";
  const tone = state === "COMPLETED" ? "good" : state === "RUNNING" ? "active" : "neutral";
  return <span className={`status-pill ${tone}`}><i />{state}</span>;
}

function AppShellError({ message, retry }: { message: string; retry: () => void }) {
  return (
    <main className="fatal-state">
      <Mark />
      <p className="eyebrow">StreamForge connection</p>
      <h1>The observatory is offline.</h1>
      <p>{message}</p>
      <button className="button primary" onClick={retry}>Try again</button>
      <small>Start the core service on 127.0.0.1:8080, then retry.</small>
    </main>
  );
}

export function App() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [datasetId, setDatasetId] = useState("");
  const [start, setStart] = useState(JAN_START);
  const [end, setEnd] = useState(FEB_START);
  const [zone, setZone] = useState("");
  const [filters, setFilters] = useState({ start: JAN_START, end: FEB_START, zone: "" });
  const [rows, setRows] = useState<HourlyZoneStat[]>([]);
  const [rejections, setRejections] = useState<RejectionRecord[]>([]);
  const [runs, setRuns] = useState<ReplayRun[]>([]);
  const [rejectionCursor, setRejectionCursor] = useState<string | null>(null);
  const [cursorHistory, setCursorHistory] = useState<(string | null)[]>([]);
  const [datasetComplete, setDatasetComplete] = useState(false);
  const [loading, setLoading] = useState(true);
  const [viewLoading, setViewLoading] = useState(false);
  const [error, setError] = useState("");
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    listDatasets(controller.signal)
      .then((page) => {
        setDatasets(page.items);
        setDatasetId((current) => current || page.items[0]?.id || "");
        setError("");
      })
      .catch((reason) => {
        if (reason instanceof DOMException && reason.name === "AbortError") return;
        setError(errorMessage(reason));
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [refreshKey]);

  const loadView = useCallback(
    (cursor: string | null = null) => {
      if (!datasetId) return () => undefined;
      const controller = new AbortController();
      setViewLoading(true);
      setError("");
      Promise.all([
        getAnalytics(datasetId, toUtc(filters.start), toUtc(filters.end), filters.zone, controller.signal),
        getRejections(datasetId, cursor, controller.signal),
        getRuns(datasetId, controller.signal),
      ])
        .then(([analytics, rejectionPage, runPage]) => {
          setRows(analytics.items);
          setDatasetComplete(analytics.dataset_complete);
          setRejections(rejectionPage.items);
          setRejectionCursor(rejectionPage.next_cursor);
          setRuns(runPage.items);
        })
        .catch((reason) => {
          if (reason instanceof DOMException && reason.name === "AbortError") return;
          setError(errorMessage(reason));
        })
        .finally(() => setViewLoading(false));
      return () => controller.abort();
    },
    [datasetId, filters],
  );

  useEffect(() => {
    setCursorHistory([]);
    return loadView(null);
  }, [loadView, refreshKey]);

  const selected = datasets.find((dataset) => dataset.id === datasetId);
  const totals = useMemo(() => {
    let trips = 0;
    let distance = 0;
    let fare = 0;
    let fareCount = 0;
    const zones = new Set<number>();
    for (const row of rows) {
      trips += row.trip_count;
      distance += row.total_distance_milli_miles;
      fare += row.total_fare_cents;
      fareCount += row.fare_observed_count;
      zones.add(row.pickup_zone_id);
    }
    return { trips, distance, fare, fareCount, zones: zones.size };
  }, [rows]);

  if (loading && datasets.length === 0) {
    return <main className="loading-screen"><Mark /><span>Opening replay observatory…</span></main>;
  }
  if (error && datasets.length === 0) {
    return <AppShellError message={error} retry={() => setRefreshKey((value) => value + 1)} />;
  }

  const nextRejections = () => {
    if (!rejectionCursor) return;
    setCursorHistory((history) => [...history, rejectionCursor]);
    loadView(rejectionCursor);
  };
  const previousRejections = () => {
    const previous = cursorHistory.length > 1 ? cursorHistory[cursorHistory.length - 2] : null;
    setCursorHistory((history) => history.slice(0, -1));
    loadView(previous);
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a className="brand" href="#top" aria-label="StreamForge home"><Mark /><span>STREAMFORGE</span></a>
        <nav aria-label="Primary navigation">
          <a className="nav-link active" href="#overview"><span>01</span>Overview</a>
          <a className="nav-link" href="#quality"><span>02</span>Data quality</a>
          <a className="nav-link" href="#provenance"><span>03</span>Provenance</a>
        </nav>
        <div className="sidebar-note">
          <span className="pulse" />
          <div><strong>Historical replay</strong><small>NYC TLC · Jan 2024</small></div>
        </div>
      </aside>

      <main id="top" className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Mobility data operations</p>
            <h1>Replay observatory</h1>
          </div>
          <button className="icon-button" aria-label="Refresh dashboard" onClick={() => setRefreshKey((value) => value + 1)}>
            ↻
          </button>
        </header>

        <section className="control-strip" aria-label="Dashboard filters">
          <label className="field dataset-field"><span>Dataset</span>
            <select value={datasetId} onChange={(event) => setDatasetId(event.target.value)}>
              {datasets.map((dataset) => <option key={dataset.id} value={dataset.id}>{dataset.filename}</option>)}
            </select>
          </label>
          <label className="field"><span>From · UTC</span><input type="datetime-local" value={start} onChange={(event) => setStart(event.target.value)} /></label>
          <label className="field"><span>To · UTC</span><input type="datetime-local" value={end} onChange={(event) => setEnd(event.target.value)} /></label>
          <label className="field zone-field"><span>Pickup zone</span><input inputMode="numeric" min="1" placeholder="All" type="number" value={zone} onChange={(event) => setZone(event.target.value)} /></label>
          <button className="button primary apply-button" disabled={viewLoading || !datasetId} onClick={() => setFilters({ start, end, zone })}>{viewLoading ? "Loading…" : "Apply"}</button>
        </section>

        {error ? <div className="notice error" role="alert"><strong>View unavailable</strong><span>{error}</span></div> : null}
        {!selected ? (
          <section className="empty-state"><p className="eyebrow">No source registered</p><h2>Register a dataset to begin.</h2><p>The dashboard reads only durable replay results from the StreamForge API.</p></section>
        ) : (
          <>
            <section id="overview" className="hero-grid">
              <article className="dataset-card panel">
                <div className="panel-heading"><div><p className="eyebrow">Selected source</p><h2>{selected.filename}</h2></div><StatusPill dataset={selected} /></div>
                <div className="source-meta">
                  <div><span>Schema</span><strong>{selected.source_type}:{selected.source_schema_version}</strong></div>
                  <div><span>Source bytes</span><strong>{formatBytes(selected.source_size_bytes)}</strong></div>
                  <div className="wide"><span>SHA-256</span><code title={selected.source_sha256}>{selected.source_sha256}</code></div>
                </div>
                <div className={`integrity-line ${datasetComplete ? "complete" : "provisional"}`}>
                  <strong>{datasetComplete ? "Complete replay evidence" : "Provisional replay evidence"}</strong>
                  <span>{datasetComplete ? "At least one run completed for this source." : "Metrics may change until a run completes."}</span>
                </div>
              </article>

              <article className="kpi panel accent-panel">
                <p className="eyebrow">Accepted trips</p><strong>{compactNumber(totals.trips)}</strong><span>{totals.zones} pickup zones represented</span>
              </article>
              <article className="kpi panel">
                <p className="eyebrow">Distance</p><strong>{compactNumber(totals.distance / 1000)}<small> mi</small></strong><span>Fixed-point source total</span>
              </article>
              <article className="kpi panel">
                <p className="eyebrow">Observed fare</p><strong>{formatMoney(totals.fare)}</strong><span>{compactNumber(totals.fareCount)} trips with fare</span>
              </article>
            </section>

            <section className="panel chart-panel">
              <div className="panel-heading"><div><p className="eyebrow">Throughput shape</p><h2>Accepted trips by pickup hour</h2></div><span className="range-label">{rows.length} hour-zone buckets</span></div>
              <TrendChart rows={rows} />
            </section>

            <section className="panel table-panel">
              <div className="panel-heading"><div><p className="eyebrow">Run ledger</p><h2>Recent replay runs</h2></div><span className="range-label">Durable per-event counters</span></div>
              <div className="table-scroll"><table><thead><tr><th>Started · UTC</th><th>State</th><th>Input</th><th>Accepted</th><th>Duplicate</th><th>Rejected</th></tr></thead>
                <tbody>{runs.map((run) => <tr key={run.id}><td>{formatUtc(run.started_at ?? run.created_at)}</td><td><span className={`run-state ${run.state.toLowerCase()}`}>{run.state}</span></td><td>{run.input_count.toLocaleString()}</td><td>{run.accepted_count.toLocaleString()}</td><td>{run.duplicate_count.toLocaleString()}</td><td>{run.rejected_count.toLocaleString()}</td></tr>)}</tbody>
              </table>{runs.length === 0 ? <p className="table-empty">No replay runs recorded for this dataset.</p> : null}</div>
            </section>

            <section className="panel table-panel">
              <div className="panel-heading"><div><p className="eyebrow">Analytics ledger</p><h2>Hourly zone aggregates</h2></div></div>
              <div className="table-scroll"><table><thead><tr><th>Pickup hour · UTC</th><th>Zone</th><th>Trips</th><th>Distance</th><th>Fare</th></tr></thead>
                <tbody>{rows.slice(0, 100).map((row) => <tr key={`${row.pickup_hour_utc}-${row.pickup_zone_id}`}><td>{formatUtc(row.pickup_hour_utc)}</td><td><span className="zone-token">{row.pickup_zone_id}</span></td><td>{row.trip_count.toLocaleString()}</td><td>{(row.total_distance_milli_miles / 1000).toLocaleString(undefined, { maximumFractionDigits: 1 })} mi</td><td>{formatMoney(row.total_fare_cents)}</td></tr>)}</tbody>
              </table>{rows.length === 0 ? <p className="table-empty">No aggregate rows match these filters.</p> : null}</div>
              {rows.length > 100 ? <p className="table-footnote">Showing the first 100 of {rows.length} returned buckets.</p> : null}
            </section>

            <section id="quality" className="panel table-panel quality-panel">
              <div className="panel-heading"><div><p className="eyebrow">Validation ledger</p><h2>Source rejections</h2></div><span className="range-label">Policy evidence, never aggregated</span></div>
              <div className="table-scroll"><table><thead><tr><th>Source row</th><th>Reason</th><th>Detail</th><th>Policy</th><th>Rejected · UTC</th></tr></thead>
                <tbody>{rejections.map((record) => <tr key={`${record.event_id}-${record.validation_policy_version}`}><td>#{record.source_row_number.toLocaleString()}</td><td><span className="reason-token">{record.reason_code}</span></td><td className="detail-cell">{record.detail}</td><td>{record.validation_policy_version}</td><td>{formatUtc(record.rejected_at)}</td></tr>)}</tbody>
              </table>{rejections.length === 0 ? <p className="table-empty">No source rejections recorded for this dataset.</p> : null}</div>
              <div className="pagination"><button className="button secondary" disabled={cursorHistory.length === 0 || viewLoading} onClick={previousRejections}>Previous</button><span>Page {cursorHistory.length + 1}</span><button className="button secondary" disabled={!rejectionCursor || viewLoading} onClick={nextRejections}>Next</button></div>
            </section>

            <section id="provenance" className="provenance">
              <div><p className="eyebrow">Interpretation guardrail</p><h2>This is a replay, not a live map.</h2></div>
              <p>Every figure is derived from the NYC TLC Yellow Taxi January 2024 historical Parquet source. Event identity is deterministic from the source SHA-256 and zero-based logical row position.</p>
            </section>
          </>
        )}
        <footer><span>STREAMFORGE 2.0</span><span>UTC throughout · durable PostgreSQL evidence</span></footer>
      </main>
    </div>
  );
}
