# Step 8 — Full Live Synthetic Acceptance and Analyst Redesign

## Purpose

Record the final acceptance evidence for the production-path synthetic execution and the analyst-facing application redesign completed on branch `feature/full-live-synthetic-acceptance`.

Acceptance date: `2026-08-05`

Verified branch baseline before this record: `9a6042ba057623e9b069c6bcd6ce299de6404dd1`

## Acceptance scope

Step 8 validates:

- all five registered synthetic scenarios through the production-path daily orchestrator;
- real OpenAI decision execution where each scenario requires AI;
- deterministic zero-AI execution for the Emirates-ID-only scenario;
- stable persisted graph, decision, frontier, AI-call, and historical-run state;
- zero-call reuse of unchanged finalized runs;
- selective reassessment after a material evidence change;
- the analyst application against real persisted acceptance state;
- mandatory node-by-node analyst review;
- the final visual and decision-presentation policy;
- the complete deterministic repository smoke-test suite.

## Live scenario acceptance

### Scenario 1 — Multi-layer suspicious and legitimate network

- Final run ID: `RUN_212582cf7ed55b8c000a`
- Real AI calls: `11`
- Persisted graph: `1` group, `108` nodes, `109` edges
- Decisions: `4` continue and `7` stop
- Termination: `FRONTIER_EXHAUSTED`
- Unchanged rerun: zero AI calls
- Historical acceptance artifacts: byte-identical after rerun
- Result: passed

### Scenario 2 — Broad common/public counterparty suppression

- Final run ID: `RUN_3b5ba49557dd7bc7710b`
- Real AI calls: `1`
- Decision: `COMMON_PUBLIC_SUPPRESS`
- Persisted graph: `1` group, `502` nodes, `501` edges
- Unchanged rerun: zero AI calls
- Historical acceptance artifacts: unchanged
- Result: passed

### Scenario 3 — Beneficiary-linked customer decisions

- Final run ID: `RUN_9c475cd8655ce63abcad`
- Real AI calls: `2`
- Decisions: `MULE_LIKE` and `INSUFFICIENT_EVIDENCE`
- Persisted graph: `1` group, `3` nodes, `2` edges
- Unchanged rerun: zero AI calls
- Historical acceptance artifacts: unchanged
- Result: passed

### Scenario 4 — Emirates-ID-only deterministic groups

- Final run ID: `RUN_b0d7999ffd1545ad5365`
- Real AI calls: `0`
- Persisted graph: `2` groups, `5` nodes, `3` edges
- Group sizes: `3` and `2`
- Customer AI assessment for EID-only links: disabled
- Termination: `FRONTIER_EXHAUSTED`
- Unchanged rerun: zero AI calls
- Result: passed

### Scenario 5 — Insufficient evidence and changed-evidence reassessment

- Initial run ID: `RUN_1cb3a21a9527572cf6ca`
- Initial real AI calls: `1`
- Initial decision: `INSUFFICIENT_EVIDENCE_SUPPRESS`
- Initial persisted graph: `1` group, `4` nodes, `3` edges
- Changed-evidence run ID: `RUN_635eb139ba3151b57c1c`
- Material evidence change: requeued exactly one counterparty reassessment
- Unrelated state: unchanged
- Cumulative historical evidence: `2` decisions and `2` AI calls
- Result: passed

## Analyst application acceptance

The analyst application was exercised against the persisted acceptance state and visually accepted with the following behavior:

- one mandatory review item for every final AI or deterministic node decision;
- seed and pending nodes excluded from required review;
- deterministic same-Emirates-ID decisions included in review;
- per-analyst review state and progress;
- one synchronized selection shared by the review queue and graph;
- queue-first review navigation with graph-based exploration;
- concise graph labels containing outcome and identifier only;
- red for suspicious or mule nodes and paths;
- green only for legitimate counterparties and their stopped paths;
- amber for non-mule customers who may be exposed or victims;
- readable `Reviewed`, `Remaining`, and `Disagreed` counters;
- analyst-facing network narrative replacing operational processing metrics;
- strongest evidence rendered as separate ordered bullet points;
- same-Emirates-ID customers receive a final mule determination based on the direct identity link;
- behavioral assessment is displayed separately and cannot override the identity-based final determination;
- deterministic EID-only graphs contain only the seed, directly linked customers, and EID relationship edges.

## Deterministic regression evidence

- Deterministic smoke tests discovered: `83`
- Deterministic smoke tests passed: `83`
- Failed tests after final alignment: `0`
- External live API calls made during deterministic regression: `0`
- The standalone billable OpenAI live-decision smoke test was intentionally excluded because real live-AI execution had already been completed and persisted through the five scenario acceptance runs.

The regression suite covered source contracts, source snapshots, synthetic providers, discovery, breadth-first orchestration, persistence, restart safety, duplicate and cycle protection, fail-closed processing, controlled reprocessing, live-acceptance contracts, analyst evidence, graph projection, review persistence, narrative presentation, same-EID policy, and the complete Streamlit interface.

## Final acceptance result

`PASSED`

Step 8 proves that the current CSV-backed synthetic provider can execute all five scenarios through the production application path, persist and reuse stable state safely, invoke real AI only where required, selectively reassess materially changed evidence, and present the resulting investigation through the accepted analyst workflow.

## Scope boundary

This acceptance does not claim production operational readiness. The following remain outside Step 8 and are carried into the later production-hardening and Databricks-source work:

- production CI and deployment controls;
- secrets management and access control;
- monitoring, alerting, and operational runbooks;
- production persistence and distributed-transaction guarantees;
- calibrated hard breadth and depth guardrails;
- replacement of the synthetic provider with Databricks source adapters.
