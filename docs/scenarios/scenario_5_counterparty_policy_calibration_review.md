# Scenario 5 Counterparty Policy Calibration Review

## Purpose

Scenario 5 produced a completed live `SUSPICIOUS_EXPAND` decision from sparse shared-counterparty evidence. The pipeline behaved correctly, but the decision exposed a calibration gap: shared usage and the absence of a recurring legitimate pattern were treated as positive suspicious evidence.

## Policy correction

Counterparty assessment policy `counterparty-assessment-policy-v2` establishes that:

- shared usage is network topology and is never sufficient by itself for `SUSPICIOUS_EXPAND`;
- suspicious expansion normally requires at least two independent corroborating evidence categories beyond shared usage;
- multiple metrics derived from the same underlying fact do not count as independent corroboration;
- generic payment-purpose text, one payment per customer, a young account, or absence of recurrence are not positive suspicious indicators;
- sparse, one-off, low-volume, or ambiguous shared activity normally maps to `INSUFFICIENT_EVIDENCE_SUPPRESS`;
- `HIGH` confidence for suspicious expansion requires multiple strongly corroborating evidence categories.

## Versioning and reassessment

The active counterparty policy version is embedded in the neutral behavioral evidence payload. Changing the policy version therefore changes the feature snapshot hash and makes the prior decision stale without deleting it from history.

The prior live `SUSPICIOUS_EXPAND` decision remains in the audit and decision store. It is no longer projected as the applied decision after the policy-versioned evidence is regenerated. Exactly one counterparty reassessment is queued, and the two downstream customer actions created by the stale decision are removed from the current frontier.

Customer prompt and decision-version compatibility remain unchanged.

## Safety expectations

Before reassessment:

- applied counterparty decision: none;
- queued counterparty AI actions: 1;
- queued customer AI actions: 0;
- recursive sources: 0;
- termination: not reached;
- live calls: 0.

The reassessment must be explicitly authorized and limited to one live call. The model remains the final decision-maker. A different completed outcome is persisted and surfaced without analyst override.
