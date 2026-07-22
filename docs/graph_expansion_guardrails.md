# Graph Expansion Guardrails

## Purpose

Recursive network discovery must not expand without explicit limits. A single
high-degree customer, counterparty, shared identifier, or loop can otherwise
produce an unbounded group, excessive AI calls, long runtimes, and an
unreviewable analyst experience.

Scenario 1 records guardrail telemetry but does not enforce synthetic-data
caps. Production thresholds will be calibrated from real group-size, degree,
branching, runtime, and AI-cost distributions before launch.

## Required production controls

Each group must have configurable soft thresholds and hard caps for:

- maximum observed traversal depth;
- maximum nodes and edges;
- maximum frontier width per phase;
- maximum new counterparties and customers admitted per round;
- maximum degree expanded from one customer or counterparty;
- maximum approved customer expansion sources;
- maximum AI calls and elapsed runtime per run.

## Required stop behavior

Hard-cap events must never silently discard work. The system must:

1. stop further expansion deterministically;
2. persist the blocked frontier and the triggering limit;
3. retain discovered evidence and all completed decisions;
4. emit a stable stop reason such as `MAX_GROUP_DEPTH_REACHED` or
   `MAX_GROUP_NODES_REACHED`;
5. support an explicitly approved resume or override;
6. show blocked counts and limits in the group summary and audit outputs.

## Loop and reuse controls

Regardless of numerical limits, the system must prevent repeated work by:

- stable group, node, edge, subject, queue, and evidence identifiers;
- a completed expansion ledger;
- feature-hash decision reuse;
- suppression of already-observed counterparties during recursive discovery;
- no automatic retry of failed-closed AI actions;
- breadth-first phase barriers between counterparty decisions, customer
  decisions, and subsequent discovery.

## Current telemetry contract

Scenario 1 writes the following per-group fields after recursive discovery:

- `max_observed_depth`;
- `unreachable_node_count`;
- `total_node_count` and `total_edge_count`;
- `current_frontier_width`;
- `expansion_source_count`;
- `new_node_count` and `new_edge_count`;
- `breadth_cap_enforced_flag` and `depth_cap_enforced_flag`;
- `guardrail_status`.

During synthetic validation, `guardrail_status` is `TELEMETRY_ONLY` and both
enforcement flags remain false.

## Calibration before production

Threshold selection must use production distributions rather than Scenario 1.
At minimum, evaluate p50, p90, p95, p99, and maximum values for group size,
node degree, depth, frontier width, runtime, and AI calls. Set soft thresholds
around unusual-but-reviewable upper-tail behavior and separate hard emergency
ceilings. Thresholds must be versioned and included in every run audit.

## Phase-level telemetry

The same telemetry contract must be emitted after recursive customer decisions,
not only after relationship discovery. This captures whether newly approved
`MULE_LIKE` customers widen the next discovery frontier even when the graph's
node and edge counts do not change during the decision phase.
