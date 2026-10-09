import type {
  AnalyticsResponse,
  ApiProblem,
  DatasetPage,
  KafkaLagStatus,
  RejectionPage,
  RunPage,
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
