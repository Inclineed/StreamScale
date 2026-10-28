// Shapes produced by console/server/streamscale_console (derive.py, app.py) and the agent.

export type SourceStatus = {
  ok: boolean;
  at: number | null;
  error: { type: string; message: string } | null;
};

export type PodPoint = {
  name: string;
  ready: boolean | null;
  phase: string | null;
  deleting: boolean;
  restarts: number;
  cpu_pct: number | null;
  cpu_request_pct: number | null;
  cpu_cores: number | null;
  cpu_limit: string | null;
  cpu_request: string | null;
  ip: string | null;
  waiting_reason: string | null;
  last_termination: { reason: string | null; exit_code: number | null; finished_at: string | null } | null;
  active_faults: string[] | null;
  run_id: string | null;
  rss_bytes: number | null;
  state: string | null;
  assigned: number[] | null;
  loop_restarts: number | null;
  last_error: { type: string; message?: string; at?: string } | null;
  scraped: boolean;
};

export type Signal = {
  id: string;
  severity: "critical" | "warning" | "info";
  component: string;
  text: string;
  evidence: string;
};

export type Point = {
  t: number;
  received_at: number;
  sequence: number;
  run_id: string | null;
  sources: Record<string, SourceStatus>;
  rates: {
    arrival: number | null;
    drain: number | null;
    acknowledged: number | null;
    accepted_sql: number | null;
    processed: number | null;
    arrival_by_partition: number[] | null;
    drain_by_partition: number[] | null;
    dead_letters: number | null;
  };
  lag: number[] | null;
  lag_total: number | null;
  members: number | null;
  group_state: string | null;
  desired: number | null;
  ready: number | null;
  pods: PodPoint[];
  facts: number | null;
  states: number | null;
  db_probe_ms: number | null;
  e2e_p95: number | null;
  db_p95: number | null;
  totals: Record<string, number>;
  failed: Record<string, number>;
  dlq_copies: number | null;
  restarts: number;
  drain_estimate_s: number | null;
  signals: Signal[];
  sale: SalePoint | null;
  mode: string | null;
  override: boolean | null;
};

export type SalePoint = {
  state: string;
  run_name: string;
  target_rate: number | null;
  attempted: number;
  acknowledged: number;
  failed: number;
  budget: number;
  rate_mode: "schedule" | "manual";
  outcome: string | null;
  buffer_full: boolean;
};

export type SaleStatus = SalePoint & {
  run_id: string;
  group_id: string;
  config: {
    scenario: string;
    seed: number;
    schedule: [number, number][];
    orders: number;
    faults: Record<string, number>;
    mode: string;
    preset: string;
  };
  purged: number;
  paused: boolean;
  stop_requested: string | null;
  started_at: number | null;
  ended_at: number | null;
  schedule_seconds: number | null;
  error: string | null;
  history: { at: number; action: string; rate?: number | null }[];
  recent: {
    event_id: string;
    order_id: string;
    event_type: string;
    fault: string;
    duplicate: boolean;
    partition: number;
    offset: number;
  }[];
  partition_acks: number[];
};

export type Preset = {
  id: string;
  label: string;
  scenario: string;
  seed: number;
  schedule: [number, number][];
  description: string;
  faults: Record<string, number>;
  events: number;
  seconds: number;
};

export type SessionRecord = {
  id: string;
  created_at: string;
  ended_at?: string;
  state: string;
  outcome?: string | null;
  run_name: string;
  run_id: string;
  config: Record<string, unknown>;
  sale_state?: string;
  reconciliation_status?: string;
  reconciliation?: Record<string, unknown>;
  comparable?: boolean;
  segments?: unknown[];
  faults?: unknown[];
};

export type SalesState = {
  presets: { presets: Preset[]; rates: { steady: number; surge: number }; pilot_capacity_per_second: number | null };
  saved: Preset[];
  session: SessionRecord | null;
  sessions: SessionRecord[];
};

export type PartitionSample = {
  partition: number;
  low: number;
  high: number;
  committed: number | null;
  position?: number;
  lag: number | null;
  run_start?: number;
  run_end?: number | null;
  run_published?: number;
  run_remaining?: number | null;
};

export type GroupMember = {
  member_id: string;
  client_id: string;
  host: string;
  partitions: number[];
};

export type PodSample = {
  name: string;
  ip: string | null;
  phase: string | null;
  ready: boolean;
  restarts: number;
  deleting: boolean;
  created_at: string | null;
  started_at: string | null;
  waiting_reason: string | null;
  terminated_now: string | null;
  last_termination: { reason: string | null; exit_code: number | null; finished_at: string | null } | null;
  cpu_limit: string | null;
  cpu_request: string | null;
  memory_limit: string | null;
  resize: string[];
};

export type Controller = {
  kind: "HorizontalPodAutoscaler" | "ScaledObject";
  name: string;
  min: number | null;
  max: number | null;
  current_replicas?: number | null;
  desired_replicas?: number | null;
  cpu_utilization?: number | null;
  cpu_target?: number | null;
  external_target?: string | null;
  group?: string | null;
  lag_threshold?: string | null;
  ready?: boolean;
  active?: boolean;
  paused?: boolean;
  owned_by_keda?: boolean;
  conditions?: { type: string; status: string; reason: string | null; message: string }[];
};

export type ConsumerScrape = {
  pod: string;
  ip: string;
  ok: boolean;
  error?: { type: string; message: string };
  metrics?: Record<string, unknown> & { failed?: Record<string, number> };
  state?: {
    state: string;
    run_name: string | null;
    run_id: string | null;
    assigned: number[];
    loop_restarts: number;
    last_error: { type: string; message?: string; at?: string } | null;
    active_faults: string[];
  };
};

export type RunSummary = {
  run_name: string;
  run_id: string;
  group_id: string;
  status: string;
  mode: string;
  scenario: string;
  seed: number;
  attempts: number;
  acknowledged: number;
  valid_unique: number;
  invalid_attempts: number;
  duplicate_attempts: number;
  publish_started_at: string | null;
  publish_ended_at: string | null;
  console?: Record<string, unknown> | null;
};

export type Sample = {
  sampled_at: number;
  sequence: number;
  sources: Record<string, SourceStatus & { duration_ms: number | null }>;
  run: RunSummary | null;
  kafka: {
    topic: string;
    partitions: PartitionSample[];
    group: { id: string; state: string; members: GroupMember[] } | null;
    dlq?: { copies: number; high: number[] };
  } | null;
  sql: { probe_ms: number; facts?: number; states?: number; status_counts?: Record<string, number> } | null;
  kubernetes: {
    consumer: { desired: number; ready: number; available: number; updated: number } | null;
    pods: PodSample[] | null;
    controllers: Controller[] | null;
    infrastructure: Record<"kafka" | "postgres", { desired: number; ready: number; pod: PodSample | null }> | null;
    resources: Record<string, { cpu_cores: number; memory_bytes: number; window: string; timestamp: string }> | null;
  };
  consumers: ConsumerScrape[];
  sale?: (SaleStatus & { state: string }) | { state: "idle" } | null;
  scaling?: Record<string, unknown> | null;
  // ML-reactive / ML-predictive controller status and recent decisions (M16).
  autoscaler?: Record<string, unknown> | null;
  faults?: Record<string, unknown> | null;
  dlq?: { codes: Record<string, number>; copies_read: number } | null;
};

export type Operation = {
  id: string;
  kind: string;
  params: Record<string, unknown>;
  key: string;
  state: "queued" | "running" | "succeeded" | "failed" | "rejected";
  created_at: number;
  started_at: number | null;
  ended_at: number | null;
  error: string | null;
  result: Record<string, unknown> | null;
  progress: { at: number; text: string }[];
  session: string | null;
};

export type JournalEntry = {
  at: number;
  type: "operation" | "system" | "note" | "observation";
  kind?: string;
  label?: string;
  op_id?: string;
  state?: string;
  text?: string;
  error?: string | null;
  params?: Record<string, unknown>;
  result?: Record<string, unknown> | null;
};

export type Workflow = {
  held_by_console: boolean;
  console_purpose: string | null;
  holder: { pid: number; started_at: string | null; purpose: string | null; command: string | null } | null;
};

export type AgentStatus = {
  agent: "connecting" | "connected" | "unreachable";
  since: number;
  error: string | null;
  last_ok: number | null;
  forward_restarts: number;
};

export type ConsoleState = {
  server: {
    started_at: number;
    port: number;
    agent: { version: number; started_at: string; pod: string; image: string; commands: string[] } | null;
    deployment: Record<string, string> | null;
    dashboards: { grafana?: string; prometheus?: string };
  };
  status: AgentStatus;
  points: Point[];
  sample: Sample | null;
  journal: JournalEntry[];
  operations: Operation[];
  workflow: Workflow;
  now: number;
  [component: string]: unknown;
};
