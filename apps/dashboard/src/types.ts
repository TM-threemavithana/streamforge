export type RunState = "CREATED" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED";

export interface Dataset {
  id: string;
  source_type: string;
  source_sha256: string;
  source_schema_version: string;
  filename: string;
  source_size_bytes: number;
  status: string;
  created_at: string;
  has_completed_run: boolean;
  latest_run_state?: RunState;
}

export interface HourlyZoneStat {
  dataset_id: string;
  pickup_zone_id: number;
  pickup_hour_utc: string;
  trip_count: number;
  total_distance_milli_miles: number;
  total_fare_cents: number;
  fare_observed_count: number;
}

export interface ReplayRun {
  id: string;
  dataset_id: string;
  state: RunState;
  started_at?: string;
  finished_at?: string;
  input_count: number;
  accepted_count: number;
  duplicate_count: number;
  rejected_count: number;
  created_at: string;
}

export interface RejectionRecord {
  dataset_id: string;
  event_id: string;
  source_row_number: number;
  reason_code: string;
  detail: string;
  validation_policy_version: string;
  rejected_at: string;
}

export interface DatasetPage {
  items: Dataset[];
  next_cursor: string | null;
}

export interface AnalyticsResponse {
  items: HourlyZoneStat[];
  dataset_complete: boolean;
}

export interface RejectionPage {
  items: RejectionRecord[];
  next_cursor: string | null;
}

export interface RunPage {
  items: ReplayRun[];
  next_cursor: string | null;
}

export interface ConsumerPartitionLag {
  topic: string;
  partition: number;
  committed_offset: number;
  end_offset: number;
  lag: number;
}

export interface ConsumerLag {
  group: string;
  state: string;
  members: number;
  total_lag: number;
  partitions: ConsumerPartitionLag[];
  observed_at: string;
}

export interface KafkaLagStatus {
  enabled: boolean;
  available: boolean;
  consumer?: ConsumerLag;
}

export interface ApiProblem {
  code?: string;
  message?: string;
  request_id?: string;
}

export interface AlertRule {
  ruleId: string;
  ruleVersion: number;
  kind: string;
  parameters: Record<string, unknown>;
  enabled: boolean;
  createdAt: string;
}

export interface Alert {
  alertId: string;
  eventId: string;
  ruleId: string;
  ruleVersion: number;
  datasetId: string;
  payload: Record<string, unknown>;
  createdAt: string;
}

export interface AlertPage {
  items: Alert[];
  page: number;
  limit: number;
  total: number;
}
