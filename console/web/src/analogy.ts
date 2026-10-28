// The console's single best-fit analogy: a supermarket checkout during a flash sale.
// Every Store-lens label maps to one real component, and each entry says where the
// analogy stops, so the picture never claims behaviour the pipeline does not have.

export type Lens = "store" | "system";

export type Term = {
  store: string;
  system: string;
  explain: string;
  limit?: string;
};

export const TERMS = {
  publisher: {
    store: "Store doors",
    system: "Interactive publisher",
    explain:
      "Shoppers arrive through the doors at the sale's rate. In the pipeline, the publisher sends order events to Kafka and records every broker acknowledgement.",
    limit: "A shopper is one event, not one person: one order normally produces two events.",
  },
  routing: {
    store: "Loyalty-card lane rule",
    system: "Key-hash partitioning",
    explain:
      "Each order's loyalty card always sends it to the same lane, so its events stay in order. The publisher keys every event by order_id.",
    limit: "Routing is a hash of the key, not a shopper's choice of the shortest lane.",
  },
  partition: {
    store: "Checkout lane",
    system: "Kafka partition",
    explain: "orders.events.v1 has exactly three partitions, like three checkout lanes.",
    limit: "Kafka keeps every record after it is read; the lane keeps a numbered ticket roll.",
  },
  high: {
    store: "Tickets issued",
    system: "High watermark",
    explain: "The next ticket number the lane will hand out: the partition's log-end offset.",
  },
  committed: {
    store: "Now serving",
    system: "Committed offset",
    explain:
      "The number on the lane's board. A consumer advances it only after the event is safely recorded.",
  },
  lag: {
    store: "Queue length",
    system: "Consumer lag",
    explain: "Tickets issued minus now serving: events published but not yet durably handled.",
  },
  consumer: {
    store: "Cashier",
    system: "Consumer pod",
    explain:
      "One replica of the same Python consumer in one consumer group. A cashier can cover several lanes.",
    limit: "A lane has at most one cashier, so a fourth cashier at three lanes would stand idle.",
  },
  assignment: {
    store: "Lanes covered",
    system: "Partition assignment",
    explain: "Which lanes each cashier serves, as reported by the Kafka group coordinator.",
  },
  rebalance: {
    store: "Lane handover",
    system: "Group rebalance",
    explain:
      "When a cashier arrives or leaves, lanes are reassigned. Everyone briefly stops; unfinished tickets are served again.",
  },
  controller: {
    store: "Floor manager",
    system: "Scaling controller",
    explain:
      "Decides how many cashiers are on duty: fixed, manual, CPU-HPA, lag-KEDA, ML-reactive or ML-predictive.",
  },
  cpuHpa: {
    store: "“Cashiers look busy” rule",
    system: "CPU HorizontalPodAutoscaler",
    explain: "Adds cashiers when their average CPU use exceeds the target share of their request.",
    limit: "A cashier waiting on the ledger does not look busy, even with a long queue.",
  },
  lagKeda: {
    store: "“Queues are long” rule",
    system: "KEDA Kafka lag scaler",
    explain: "Adds cashiers when the queue per cashier exceeds the lag threshold.",
  },
  database: {
    store: "Sales ledger",
    system: "PostgreSQL",
    explain:
      "order_event is the append-only ledger of accepted facts; order_state is the board showing each order's latest status.",
  },
  dlq: {
    store: "Problem-items desk",
    system: "Dead-letter queue",
    explain:
      "Unreadable items go to the desk with a written reason (orders.events.dlq.v1). The cashier moves on only after the desk confirms receipt.",
  },
  commitOrder: {
    store: "Ledger first, then the board",
    system: "DB commit before offset commit",
    explain:
      "A cashier records the sale in the ledger before advancing “now serving”. If they collapse in between, the next cashier serves that ticket again.",
    limit: "That replay is expected at-least-once behaviour, not exactly-once delivery.",
  },
  duplicate: {
    store: "Receipt-number check",
    system: "event_id duplicate detection",
    explain: "A receipt number already in the ledger is recognised, so a replayed or repeated event is stored once.",
  },
  late: {
    store: "Late slip",
    system: "Late event",
    explain:
      "A slip that arrives after a newer one is filed in the ledger history, but cannot roll the status board backwards.",
  },
  latency: {
    store: "Wait time",
    system: "End-to-end latency",
    explain: "From the event's produced_at to its committed SQL fact.",
  },
} satisfies Record<string, Term>;

export type TermId = keyof typeof TERMS;

export const MODE_LABELS: Record<string, Term> = {
  fixed: {
    store: "One cashier, always",
    system: "Fixed · 1 replica",
    explain: "No autoscaler. The baseline the experiment compares against.",
  },
  manual: {
    store: "You call cashiers in",
    system: "Manual · 0–3 replicas",
    explain: "No autoscaler; you set replicas directly. Zero pauses processing; it is not a crash.",
  },
  "cpu-hpa": {
    store: "“Cashiers look busy” rule",
    system: "CPU-HPA · 1–3 replicas",
    explain: "Kubernetes adds or removes replicas from average CPU utilisation.",
  },
  "lag-keda": {
    store: "“Queues are long” rule",
    system: "Lag-KEDA · 1–3 replicas",
    explain: "KEDA adds or removes replicas from Kafka consumer-group lag.",
  },
  "ml-reactive": {
    store: "Manager watching the door",
    system: "ML-reactive · 1–3 replicas",
    explain:
      "Every second, simulates the queues for 1, 2 and 3 cashiers at today's arrival rate and picks the fewest that keep waits under 3 s.",
  },
  "ml-predictive": {
    store: "Manager who expects the rush",
    system: "ML-predictive · 1–3 replicas",
    explain:
      "Same one-second planner, but plans with a learned forecast of the next 20 s of arrivals and learns cashier speed and start-up time as it goes.",
  },
};

export const ML_MODES = ["ml-reactive", "ml-predictive"] as const;

export function label(id: TermId, lens: Lens) {
  return TERMS[id][lens];
}

export function caption(id: TermId, lens: Lens) {
  return TERMS[id][lens === "store" ? "system" : "store"];
}
