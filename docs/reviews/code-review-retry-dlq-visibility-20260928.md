# Code review — Retry, DLQ, SNS, visibility, and recovery (issue #7)

- **Date:** 2026-09-28
- **Fixed point:** `3cfb9ce` (branch `aws-workflow` HEAD) — uncommitted working tree
- **Diff:** `git diff 3cfb9ce`
- **Reviewed artifacts:** `etl/ingestion.py` (`RejectedObjectError`, `SqsMessage.attempt`, `MAX_DELIVERY_ATTEMPTS`, `VISIBILITY_TIMEOUT_SECONDS`/`VISIBILITY_HEARTBEAT_SECONDS`, `SQSAdapter.change_message_visibility`, `ApproximateReceiveCount` parsing, `_record_terminal`, `_alert_rejected`, `_settle_failure`, `_rejection`, `_PendingBoundary`/`_SourceSucceeded`, `_VisibilityHeartbeat`, `_release_visibility`, `run_worker`), `etl/transform.py` (`SourceValidationError` propagation, `_reported`), `tests/test_ingestion.py` (the issue #7 section, plus eleven tests whose expectations the new terminal semantics changed), `CONTEXT.md` (**Rejected object version**, **Delivery attempt**), `TechnicalSpecification.md` (Retries/rejection/DLQ/alerting, `stage_results.stage`), `AWS.md` (§2 retry contract, §4), `Lineage.md` (§4.1 flow and retry contract, §4.2, §6)
- **Spec:** issue #7, *Add retry, DLQ, SNS, visibility, and recovery behavior* (parent #1); scope boundaries #8 (operator startup / Compose) and #10 (Terraform for the queue, redrive policy, and alarm), both open.
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/agents/domain.md`, `docs/adr/*`, `TechnicalSpecification.md`, prior reports in `docs/reviews/` (notably `code-review-mixed-message-ordering-20260928.md` and `code-review-source-snapshot-slice-20260927.md`)
- **Verification:** `.venv/bin/python -m pytest` — **430 passed**; `pyflakes` shows no new findings; `python -m compileall -q etl tests viz` clean

Both axes were run as parallel sub-agents over the same diff, then their findings were
actioned. This report records what they said and what was done about it.

## Standards

1. **High — hard breach (house convention, `code-review-source-snapshot-slice-20260927.md:82-85`: "a default captured in the signature would silently defeat them" — module constants must be rebindable via `monkeypatch.setattr`) — `etl/ingestion.py:1277`** `interval: float = VISIBILITY_HEARTBEAT_SECONDS` froze the constant at import, so the test's rebinding was a no-op. → **Fixed**: the parameter is gone and `__init__` reads the module global, so the seam the tests use is the seam the code uses.
2. **Medium — Speculative Generality / dead seam — `etl/ingestion.py:1277`** No caller passed `interval`; it existed only for the broken test seam. → **Fixed** with the above.
3. **Medium — Duplicated Code + Data Clump — `etl/ingestion.py:1136, 1160, 1173, 1201, 1214, 1236`** Six copies of the same eight-line `_settle_failure(...)` block, and the `successes` 5-tuple unpacked positionally in three places. → **Fixed**: a local `fail(...)` closure closes over the engine, the delivery attempt and SNS, and `_PendingBoundary`/`_SourceSucceeded` name the two tuples.
4. **Medium — Mysterious Name / parameter reuse — `etl/ingestion.py:997`** `error = RuntimeError("delivery attempts exhausted…")` overwrote the real cause, so `terminal_error` lost e.g. `"transient S3 read failure"`. → **Fixed**: the cause is kept and the exhaustion note is appended to it, and the test asserts both halves.
5. **Low — Mysterious Name — `tests/test_ingestion.py`** `_alerted_runs` returned `ingestion_runs` rows, nothing about alerts. → **Fixed**: renamed `_runs_for_key`.
6. **Low — glossary drift (`docs/agents/domain.md:41-45`)** The canonical term is **Rejected object version**; the docs said "rejected file", next to the `_Avoid_` list. → **Fixed** in `AWS.md`, `Lineage.md` and the `_Avoid_` list.
7. **Low — spec formatting — `TechnicalSpecification.md`** The rewritten `stage` row broke the table's padding. → **Fixed**.
8. **judgement — Primitive Obsession — `SqsMessage.attempt: int = 1`** Left: the default documents "first delivery", the boto3 adapter always sets the parsed count, and `test_boto3_sqs_reports_the_delivery_attempt_and_extends_visibility` pins the parse. Removing it would only move the obligation to the fakes.
9. **judgement — Divergent Change — `_VisibilityHeartbeat` in a module about ingest/ledger orchestration** Declined: the module already owns the receive loop and the queue adapters, so the class is where its only consumer can find it. A separate `queue_visibility.py` would be one class and a one-way import.
10. **Conventions checked and matching:** `RejectedObjectError`/`_record_terminal`/`_alert_rejected` naming, `log.error`/`log.exception` with `%s` args, imperative-summary-then-prose docstrings, full annotations, and the shared fixtures instead of a hand-rolled schema harness (AGENTS.md's hermetic-suite rule). The one divergence — the default captured in a signature — is item 1.

## Spec

1. **High — AC4 "Exhausted infrastructure messages move to the DLQ…"** Nothing lowered the visibility the heartbeat had just raised, so SQS could not apply the redrive policy for up to six hours after the fifth delivery (and after each earlier retry). → **Fixed**: `run_worker` hands the message back with `ChangeMessageVisibility: 0` in a `finally` whenever it did not acknowledge it, so a retry or the redrive happens then. `test_an_unacknowledged_message_is_handed_back_to_the_queue` covers the normal retryable return, and the crash test covers the exception path.
2. **High — AC1 "Deterministic file errors publish an SNS alert immediately"** Only the *inspect* step mapped `SourceValidationError` to a rejection. A row with neither a Reference ID nor a location passes inspect and fails deterministically in the transform — and `transform_source_snapshot` swallowed the error into `report.errors`, so it was recorded *retryable*: five deliveries, no alert. → **Fixed**: `SourceValidationError` now leaves `_transform_source` (after discarding unverified staging, as the error path did) and the processor's per-stage `except` maps it to `_rejection` with the stages that passed; `transform_sources` catches it into the report so the hand-run CLI still reports instead of raising. `test_a_rejection_found_later_in_the_chain_is_also_terminal` covers it.
3. **Medium — AC7 "Tests simulate … crash/redelivery"** The crash was raised before `process` did any work, so "keeps its half-finished work" was untested, and no case went through `run_worker`. → **Fixed**: the crash is injected between extract and transform, driven through `run_worker`, and the test asserts the open run, the raw version left behind, the message handed back, the redelivery succeeding, exactly one raw version afterwards, and the two-attempt stage history.
4. **Low — AC1/AC6** `_alert_rejected` runs after the terminal write, so a crash or an SNS outage there loses the alert permanently. → **Accepted, documented**: the run is recorded terminal *before* the alert so a redelivery cannot duplicate it (AC6). The trade is a rare lost alert over never a double alert; the docstring and `AWS.md` say so. A failed *send* is now logged and does not hold up the message (`test_a_failed_alert_does_not_hold_up_the_rest_of_the_message`).
5. **Low — AC5** An exhausted run is stored as `retryable` with a `terminal_error`, so it looks like a first-attempt failure. → **Accepted, and now reasoned in the spec**: `attempt_count` plus the exhaustion note distinguish it, and `retryable` is what keeps the run *resumable* — an operator who redrives from the DLQ after fixing the cause continues the same run instead of finding it settled and ignored.
6. **Low — AC1** A batch rejection alerts and terminates every level of the release, so one mis-keyed file sends several alerts. → **Accepted, and now stated in the spec**: the Boundary release is all-or-nothing by design (#5), the levels are only valid as a set, and each rejected version gets its own alert naming its own reason.
7. **No scope creep:** `bootstrap()` is untouched, no Terraform or Compose changes, `Lineage.md` §6 defers #8 and #10, and the delivery count genuinely comes from `ApproximateReceiveCount` — `attempt_count` is never used for exhaustion.

### One finding about the previous review's own test

While fixing item 1 the reviewer of record noticed, by instrumentation, that
`test_a_slow_message_keeps_its_visibility_extended` (issue #6) had been passing
vacuously: the announced version was not the one the fake S3 served, so the
message settled `stale` on arrival, no slow work ran, and only the single
`__enter__` beat was ever taken. The test now publishes the version S3 serves and
asserts that the record was processed and that the beat *repeats*.

## Summary

Standards: 7 findings (1 hard breach, 3 medium, 3 low), all fixed; 2 judgements
declined with reasons; the worst was the signature-captured heartbeat constant
that made the #7 heartbeat seam a no-op. Spec: 6 findings (2 High, 1 Medium, 3
Low), the two High ones — an exhausted message parked for six hours and a
transform-time validation error retried five times unalerted — both fixed; the
worst was the missing visibility release, which would have made AC4's redrive
happen by accident rather than by design.
