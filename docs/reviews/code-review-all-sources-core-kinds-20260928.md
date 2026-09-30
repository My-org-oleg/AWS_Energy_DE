# Code review — All Source datasets and Core kinds (issue #4)

- **Date:** 2026-09-28
- **Fixed point:** `e6e3d17` (branch `aws-workflow` HEAD) — uncommitted working tree
- **Diff:** `git diff e6e3d17`
- **Reviewed artifacts:** `etl/source_data.py` (duplicate Reference ID check),
  `etl/load.py` (per-kind `verified_columns`, generic authoritative
  verification, `_same_value`), `etl/transform.py` + `etl/db_utils.py`
  (discard unverified staging after a failed snapshot transform),
  `TechnicalSpecification.md`, `tests/test_extract.py`, `tests/test_ingestion.py`
- **Spec:** issue #4, *Complete all Source datasets and Core kinds* (parent #1).
  Sibling slices (Boundary releases, retry/DLQ/SNS classification, bootstrap
  enqueueing, visualization timescopes, Terraform) were out of scope.
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/agents/domain.md`,
  `docs/adr/*`, `TechnicalSpecification.md`, prior reports in `docs/reviews/`
- **Verification:** `.venv/bin/python -m pytest tests/` — **391 passed**
  (0 failed, 0 skipped), plus `python -m compileall -q etl tests viz`. A one-off
  uncommitted smoke run published all six real V20260203 datasets (81,655 rows)
  in one SQS message through the worker to a scratch database: six runs
  succeeded, both Core kinds populated, marts verified with zero drift (~200 s).

## Standards

No hard breaches of a documented standard. The first pass raised 2 misleading
comments, 1 coverage gap and 6 judgement calls:

- **Misleading docstring — fixed.** The new duplicate-with-null test claimed a
  validation error would *not* be retried; the worker still records it
  `retryable` (terminal classification is a sibling slice). Reworded to the real
  difference: a failed extract stage result with a clear message instead of an
  unexplained pandas crash.
- **Coverage gap — fixed.** Verification reports only the first mismatching
  column, so the storage test proved only `storage_type`. It is now
  parametrized over `storage_type` and `storage_capacity`.
- **Duplicated Code (verified-column tuples) — fixed.** Shared
  `_SHARED_VERIFIED_COLUMNS`; storage extends it.
- **Latent `_same_value` traps — documented.** Every verified column is scalar
  (float/timestamp/text), so there is no live bug; the docstring now restricts it
  to scalar columns.
- **Mid-file imports, `kind.__dict__` — fixed.** Imports moved to the top;
  `dataclasses.replace` used.
- **Repeated Switches in test helpers — fixed.** `_core_kind` /
  `_properties_tables` now read `load.GENERATOR_KIND` / `STORAGE_KIND`.
- **Duplicated worker calls / raw-count query — fixed.** `_deliver` and
  `_raw_versions` helpers.
- **Magic number — fixed.** The raw count is scoped to the Source and commented.
- **Placement of `_same_value` in `load.py` — open, accepted.** The authoritative
  verifier already lived in `load.py` before this slice; moving it is unrelated
  churn.
- **`_CAPACITY_COLUMN` in tests repeats the gas rename — open, accepted.** Tests
  state the published-GPKG contract independently of the transform's mapping on
  purpose.

## Spec

No acceptance criterion was missing outright; the shared path from #3 already
routed every dataset. Findings:

1. **Wrong — fixed: a failed Source's staging leaked into Core.** Transform
   drops and rewrites staging *before* verifying it, and the whole-kind load
   reads every staging table present. So an unverified gas snapshot was upserted
   by the next wind/bio load. Reproduced red by
   `test_worker_never_loads_a_failed_sources_staging_through_another_source`
   (gas Core capacity became 999.0). Now a failed snapshot transform drops the
   Source's staging tables, so a present staging table always means "verified".
   The spec's Transform section records the rule.
2. **Wrong — fixed (found by the new tests, before review).** A snapshot with
   both null and duplicated non-null Reference IDs (the Solar shape) crashed
   `_validate_reference_ids` with a pandas `IndexingError`. It was recorded as a
   generic retry with no stage result instead of a clear rejection.
3. **Partial — fixed: storage-specific verification.** Post-load verification
   checked only capacity and reference date, so stale `storage_type` /
   `storage_capacity` passed as verified. Now covered by `verified_columns`.
4. **Partial — fixed: worker-path collision coverage.** Close-location,
   onshore-in-sea (wind exempt) and null `storage_capacity` are now asserted per
   Source on the worker path, alongside outside-location, bad quality, synthetic
   identity and membership.
5. **Partial — fixed: marts once for a mixed message.** A message with a
   failing gas record and a good wind record now asserts one marts refresh, wind
   succeeded and the message kept.
6. **Partial — fixed: independent mart reconciliation.** Generation capacity is
   now checked against the good fixture rows independently of the mart SQL (the
   storage mart already was).
7. **Open, follow-up: per-record whole-kind reload.** Each Source record reloads
   its complete Core kind, so a six-Source message upserts generators five times.
   That meets "loads the complete affected Core kind" but costs ~200 s on real
   data. Grouping loads by kind per message is a candidate optimisation; it
   changes per-run load stage results, so it was not taken in this slice.
8. **Informational:** validation failures remain `retryable`; terminal
   classification plus the SNS alert is the retry/DLQ sibling slice.

## Summary

- **Standards:** 9 findings (0 hard, 2 misleading comments, 1 coverage gap,
  6 judgement calls). The worst was the storage-verification coverage gap. 7
  fixed, 2 accepted with reasons.
- **Spec:** 8 findings (2 wrong, 4 partial, 1 follow-up, 1 informational). The
  worst was unverified staging of a failed Source reaching Core through another
  Source's load. All 6 wrong/partial findings fixed.
