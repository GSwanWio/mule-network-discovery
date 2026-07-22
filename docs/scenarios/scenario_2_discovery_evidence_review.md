# Scenario 2 discovery and bounded-evidence review

## Purpose

Scenario 2 validates that one common/public external account can be fully
observed without sending its entire high-degree customer population to the AI
model or opening a customer-assessment frontier before the counterparty
decision is made.

## Expected deterministic discovery

- Confirmed seed customers: 1
- Same-EID links: 0
- Candidate counterparties: 1
- Non-seed customers linked through the counterparty: 500
- Unified nodes: 502
- Unified edges: 501
- Customers blocked pending counterparty AI: 500
- Customer AI actions before the counterparty decision: 0

## Bounded evidence contract

The complete graph remains persisted. The counterparty AI snapshot contains:

- Aggregate relationship counts for all 501 counterparty relationships.
- A SHA-256 digest of the complete relationship set.
- All anchor relationships followed by a deterministic activity-ranked sample.
- At most 10 relationship records in the model payload.
- Aggregate behavioral evidence for all 501 linked customers.
- A SHA-256 digest of the complete customer behavior population.
- At most 10 representative customer behavior records in the model payload.

The full-set digests ensure that a change to an omitted relationship or linked
customer still changes the feature snapshot hash and prevents stale decision
reuse.

## Completion gate for this slice

- One counterparty AI action is queued.
- No customer AI action is queued.
- No live AI call is made.
- Scenario 1 regression tests remain green.

The following slice will execute the live counterparty decision and validate
`COMMON_PUBLIC_SUPPRESS`, zero customer exposure, clean termination, and zero
unchanged repeated calls.
