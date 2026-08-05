# Step 9 Production Hardening Inventory

Baseline branch: `feature/production-hardening`

Baseline commit: `5b58e92c90c46efe912ec0ccd3f1d41eb2f718c0`

Inventory date: `2026-08-05`

## Purpose

Record the production controls already implemented, identify the remaining gaps, and define the ordered hardening work without changing the accepted discovery or analyst behavior from Step 8.

## Existing controls

### Source and execution contracts

- One provider-neutral production entry point.
- Nine-dataset source validation before discovery.
- Provider, dataset, run-date, and state-namespace identity validation.
- Deterministic source snapshot hashing.
- Counterparty-first and customer-second breadth-first phase barriers.
- Fail-closed expansion behavior.
- Restart and unchanged-evidence reuse contracts.

### Live-AI controls

- Explicit live-AI enablement gate.
- Daily and per-run call limits.
- Required OpenAI API key loaded from the environment.
- Model, prompt version, timeout, output limit, SDK package, and SDK version validation.
- Non-secret runtime identity persisted for audit.
- Startup failures persisted with API-key redaction.
- AI calls recorded in a persistent call ledger.
- No automatic retry of failed-closed AI actions.

### State integrity and resilience

- Stable group, node, edge, queue-item, evidence, and decision identifiers.
- Duplicate node, edge, decision, queue, call, and reprocessing-key validation.
- Persisted frontier, expansion, AI-call, and technical-reprocessing ledgers.
- Atomic JSON replacement for runtime and failure artifacts.
- Final operational-resilience gate.
- Restart-safe and terminal-state idempotency coverage.
- Full Step 8 live acceptance and 83 deterministic smoke tests.

### Repository hygiene

- Production dependencies are version-pinned in `requirements.txt`.
- Virtual environments, Python build output, local environment files, test output, generated runtime state, and analyst review state are excluded by `.gitignore`.
- The accepted production-path contract, Databricks boundary, graph-guardrail requirements, and Step 8 acceptance evidence are documented.

## Remaining production gaps

### 1. Continuous integration and merge gates

Current gap:

- No repository GitHub Actions workflow.
- No single supported command for the complete deterministic suite.
- No automated compile, formatting-integrity, dependency-install, or smoke-test gate on pull requests.
- No automated secret or dependency scanning configured in the repository.

Required outcome:

- A deterministic CI workflow that installs pinned dependencies, compiles production code, runs every non-live smoke test, verifies the live test remains excluded, and fails on the first regression.
- A stable local runner used by both developers and CI.
- Documented required checks for branch protection.

### 2. Configuration and secrets contract

Current gap:

- Environment variables are validated across separate modules rather than through one application-level configuration contract.
- No committed `.env.example` or production configuration reference.
- State-directory, provider, analyst-app, AI, and future guardrail configuration are not validated together before execution.
- Secret sourcing, rotation, and access ownership are not documented.

Required outcome:

- One validated, non-secret application configuration object.
- A complete environment-variable reference and safe example file.
- Explicit startup validation before state mutation or external calls.
- Documented secret-management boundary with no secret persistence.

### 3. Enforced graph-expansion guardrails

Current gap:

- Guardrail fields are currently telemetry-only.
- Breadth and depth enforcement flags remain false.
- No hard stop currently persists blocked frontier work and a limit-specific termination reason.
- Production threshold values have not been calibrated.

Required outcome:

- Versioned soft thresholds and hard caps for depth, nodes, edges, frontier width, per-round admissions, node degree, expansion sources, AI calls, and elapsed runtime.
- Deterministic stop reasons.
- Persisted blocked work, triggering limit, and approved-resume mechanism.
- Group and analyst audit visibility.
- Calibration procedure using production distributions before launch.

### 4. Monitoring, health, and alerting

Current gap:

- Audit artifacts exist, but there is no unified run-health summary or operational metric interface.
- No structured event/logging contract.
- No alert thresholds or ownership for failed runs, failed-closed frontier work, guardrail stops, source failures, budget exhaustion, or abnormal runtime.
- No service health endpoint or scheduled-run status contract.

Required outcome:

- A persisted production run-health report.
- Structured operational events with stable codes.
- Alert conditions and severity.
- Health/readiness checks suitable for the eventual deployment target.
- Analyst-visible failure and partial-completion status.

### 5. Persistence and concurrency hardening

Current gap:

- The validated implementation uses local CSV and JSON state.
- Production storage location, locking, concurrent-run behavior, backup, retention, and disaster recovery are not defined.
- Multi-process and distributed atomicity are outside the current file replacement guarantees.

Required outcome:

- A production state-store contract independent of local files.
- Explicit single-writer or locking policy.
- Backup, retention, recovery, and historical immutability rules.
- Migration path that preserves the existing state contracts.

### 6. Deployment, access control, and runbooks

Current gap:

- No deployment artifact or service definition.
- No production entrypoint wrapper or scheduler contract.
- No access-control model for running discovery or submitting analyst reviews.
- No operational runbook, rollback process, incident procedure, or recovery checklist.

Required outcome:

- Documented deployment target and entrypoints.
- Least-privilege runtime and analyst roles.
- Startup, shutdown, retry, recovery, rollback, and incident runbooks.
- Release and environment-promotion procedure.
- Production acceptance checklist.

## Ordered Step 9 plan

1. Add the deterministic test runner and GitHub Actions CI gate.
2. Centralize and document configuration and secrets handling.
3. Implement and test enforceable graph-expansion guardrails.
4. Add run-health reporting, structured events, and alert contracts.
5. Define the production persistence and concurrency contract.
6. Add deployment, access-control, and operational runbooks.
7. Execute the complete hardening acceptance suite and merge Step 9.

## Scope protection

Step 9 must not alter the accepted decision policy, discovery sequencing, persisted logical identifiers, analyst review requirements, red/green/amber graph semantics, network narrative, evidence presentation, or same-Emirates-ID final mule precedence unless a regression or security defect requires an explicitly reviewed change.
