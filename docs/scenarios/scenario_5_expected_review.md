# Scenario 5 — Insufficient counterparty evidence

## Purpose

Validate that a low-volume shared counterparty remains visible in the observed graph but is suppressed from expansion when evidence is insufficient.

## Initial phase

- One confirmed retail seed.
- Two non-seed linked customers: one retail and one SME.
- One shared local counterparty.
- Three low-value outward payments in total.
- No EID links, rapid drain, known-mule beneficiary, public-hub pattern, or supplied AI outcome.

Expected offline decision: `INSUFFICIENT_EVIDENCE_SUPPRESS`.

The decision blocks both non-seed customer branches, creates no recursive source, and allows `FRONTIER_EXHAUSTED` termination while preserving the four-node, three-edge observed graph.

## Changed-evidence phase

Three later, materially larger payments are added. The full evidence hash must change and requeue only the counterparty assessment. The changed phase does not prescribe the next decision; the AI remains authoritative.
