import type {
  AnalyticsResponse,
  ApiProblem,
  DatasetPage,
  KafkaLagStatus,
  RejectionPage,
  RunPage,
  AlertRule,
  AlertPage,
} from "./types";

const API_BASE = (import.meta.env.VITE_STREAMFORGE_API_BASE as string | undefined)?.replace(/\/$/, "") ?? "";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly requestId?: string,
  ) {
    super(message);
  }
}

async function getJSON<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    const problem = (await response.json().catch(() => ({}))) as ApiProblem;
    throw new ApiError(problem.message ?? `Request failed with ${response.status}`, response.status, problem.request_id);
  }
  return response.json() as Promise<T>;
}

async function patchJSON<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!response.ok) {
    const problem = (await response.json().catch(() => ({}))) as ApiProblem;
    throw new ApiError(problem.message ?? `Request failed with ${response.status}`, response.status, problem.request_id);
  }
  return response.json() as Promise<T>;
}

export function listDatasets(signal?: AbortSignal) {
  return getJSON<DatasetPage>("/api/v1/datasets?limit=100", signal);
}

export function getAnalytics(
  datasetId: string,
  start: string,
  end: string,
  zone: string,
  signal?: AbortSignal,
) {
  const query = new URLSearchParams({ dataset_id: datasetId, start, end, limit: "1000" });
  if (zone) query.set("pickup_zone_id", zone);
  return getJSON<AnalyticsResponse>(`/api/v1/analytics/zone-hourly?${query}`, signal);
}

export function getRejections(datasetId: string, cursor: string | null, signal?: AbortSignal) {
  const query = new URLSearchParams({ dataset_id: datasetId, limit: "25" });
  if (cursor) query.set("cursor", cursor);
  return getJSON<RejectionPage>(`/api/v1/quality/rejections?${query}`, signal);
}

export function getRuns(datasetId: string, signal?: AbortSignal) {
  const query = new URLSearchParams({ dataset_id: datasetId, limit: "10" });
  return getJSON<RunPage>(`/api/v1/runs?${query}`, signal);
}

export function getKafkaLag(signal?: AbortSignal) {
  return getJSON<KafkaLagStatus>("/api/v1/operations/kafka-lag", signal);
}

export function getAlertRules(signal?: AbortSignal) {
  return getJSON<AlertRule[]>("/api/v1/alerts-service/rules", signal);
}

export function getAlerts(
  datasetId?: string,
  ruleId?: string,
  page: number = 0,
  limit: number = 25,
  signal?: AbortSignal,
) {
  const query = new URLSearchParams({ page: String(page), limit: String(limit) });
  if (datasetId) query.set("dataset_id", datasetId);
  if (ruleId) query.set("rule_id", ruleId);
  return getJSON<AlertPage>(`/api/v1/alerts-service/alerts?${query}`, signal);
}

export function toggleAlertRule(
  ruleId: string,
  version: number,
  enabled: boolean,
  signal?: AbortSignal,
) {
  return patchJSON<AlertRule>(
    `/api/v1/alerts-service/rules/${encodeURIComponent(ruleId)}/versions/${version}/status`,
    { enabled },
    signal,
  );
}
