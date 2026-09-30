# Code review — Boundary release replacement and geography rebuild (issue #5)

- **Date:** 2026-09-28
- **Fixed point:** `02ac121` (branch `aws-workflow` HEAD) — uncommitted working tree
- **Diff:** `git diff 02ac121` plus the untracked `etl/boundaries.py` and
  `tests/test_boundaries.py`
- **Reviewed artifacts:** `etl/boundaries.py` (content validation, atomic level
  replacement, geography rebuild), `etl/ingestion.py` (`process_boundaries`,
  Boundary-first message handling, `_claim`/`_results_for`),
  `etl/transform.py` (shared `enrich_geography`, `reenrich_staging`),
  `etl/load.py` (`GeographyRebuild`, `rebuild_core_geography`),
  `etl/verify.py` (`_verify_boundaries_on`), `TechnicalSpecification.md`,
  `CONTEXT.md`, `tests/test_boundaries.py`, `tests/test_ingestion.py`
- **Spec:** issue #5, *Publish a new Boundary release as a safe geography
  change* (parent #1). The mixed-message semantics of sibling #6 were treated as
  out of scope except where issue #5's own "one atomic Boundary batch"
  criterion required them.
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/agents/domain.md`,
  `docs/adr/*`, `TechnicalSpecification.md`, prior reports in `docs/reviews/`
- **Verification:** `.venv/bin/python -m pytest tests/` — **410 passed**
  (0 failed, 0 skipped), plus `python -m compileall -q etl tests viz` and
  `pyflakes` on the changed modules (one pre-existing unused-import warning in
  `etl/load.py`, present at the fixed point). A one-off uncommitted smoke run
  loaded all six real V20260203 datasets, then published the real
  `germany_states.gpkg` as a level-1 release: the release succeeded, 81,643
  staging rows and all 80,295 generator plus 1,348 storage Core rows were
  re-enriched, marts verified with zero drift (~52 s).

## Standards

No hard breaches of a documented standard. The first pass raised 1 correctness
bug, 1 missing term and 5 judgement calls:

- **Correctness — fixed: a failed rebuild step was reported as a success.**
  `rebuild_geography` recorded errors for a target whose row count it never set,
  and `process_boundaries` builds its results by iterating the row-count maps,
  so a staging or Core rebuild that raised produced no result at all: the batch
  succeeded and the message was acknowledged with units still carrying the old
  geography. Targets are now registered with a count of 0 before their work
  starts, and the unreadable-boundaries case fails every known target.
  `test_a_failed_geography_rebuild_fails_the_boundary_release` reproduces it
  red and now also proves the redelivery path: the release is not applied twice,
  only the geography is redone.
- **Missing term — fixed.** *Geography rebuild* is now a CONTEXT.md glossary
  entry, since the spec and the code both use it as a core term.
- **Mysterious Name — fixed.** `SourceSnapshotProcessor` handled Boundaries too;
  renamed to `PipelineProcessor`.
- **Middle Man — fixed.** The one-line `_failing_stage` wrapper is gone; callers
  use `_failing_stage_of`.
- **Duplicated Code (ledger SQL) — open, accepted.** `replace_boundary_levels`
  writes `loaded_files` itself rather than calling `extract._log_s3_load`,
  because the write has to join the release transaction. Reusing the helper
  means giving it a connection parameter, which is churn for the sake of it here.
- **Duplicated Code (enrich-and-write) — open, accepted.**
  `reenrich_staging` and `rebuild_core_geography` share the
  read-enrich-update-verify shape but write to different tables, column sets and
  verification targets. They have already drifted harmlessly; extracting a
  helper now would be a refactor of working, reviewed code for no behaviour
  change.
- **Placement of `enrich_geography` in `transform` — open, accepted.**
  `etl/load.py` now imports it, giving a load → transform edge, and a dedicated
  `etl/geography.py` cannot hold it while `etl/boundaries.py` imports `load`
  (the modules would still be one cycle away). The transform is the stage that
  owns the spatial join, so it keeps the function.
- **Output argument of `_claim` — open, accepted.** It both returns the claimed
  run and appends the settled state to the caller's list, mirroring the
  pre-existing accumulation style of `process_one_message`. Its docstring states
  the contract.
- **`ST_IsValid` added to boundary verification — informational.** The spec
  requires valid geometry; checking it in the database as well as in Python
  guards a release that a manual import bypassed.

## Spec

All six acceptance criteria are met; nothing was missing outright. Findings:

1. **Wrong — fixed: silent rebuild failures (above).** The criterion "every
   Source is re-enriched and Core values are rebuilt" was not actually
   observable: a rebuild that threw was indistinguishable from a rebuild that had
   nothing to do.
2. **Partial — fixed: a rebuild failure is retryable and the retry is safe.**
   The boundary rows are committed before the rebuild runs, so the interesting
   case is the redelivery. The release is skipped when every object version is
   already in `loaded_files`, and the rebuild runs anyway, so a redelivery
   converges. This is now covered by a test and documented on
   `process_boundaries`.
3. **Partial, by design: the local CLI path does not rebuild.** `python -m etl
   boundaries` still loads a level through the bootstrap-era extract path and
   does not re-enrich. The event-driven release is the operator-facing path in
   this slice; wiring the CLI to the same replacement is a follow-up (issue #8
   owns boundary bootstrap).
4. **Informational: the tie-break is now deterministic.** The spatial join takes
   the alphabetically first polygon name instead of the first row, so staging
   enrichment, the Core rebuild and the marts agree on border units. This is
   recorded in the spec's Boundary release section and is a behaviour change
   from the previous `keep="first"`.
5. **Informational: overlap with #6.** Grouping the Boundary records of a
   message and running them before the Source records is required by #5's atomic
   batch criterion, so #6's ordering/grouping acceptance criteria are largely
   implemented here; what remains for #6 is per-record table isolation, the
   independent-failure semantics and its own integration test.
6. **Open, follow-up: the Core rebuild is not one transaction.** The geography
   UPDATE commits, then the collision reset, detection and property links each
   run in their own transaction, so a crash in that window leaves Core on the
   new geography with collisions unannotated. A redelivery repairs it, and the
   marts verification catches the drift, but making the Core rebuild atomic is
   the remaining hardening. Out of scope here because it changes the collision
   helpers' signatures, which the Source load path also uses.
7. **Informational: validation failures stay retryable**, consistent with the
   retry/DLQ sibling slice; the release is simply not applied.

## Summary

- **Standards:** 8 findings (1 correctness bug, 1 missing term, 2 naming/
  redundancy, 4 judgement calls). 3 fixed, 5 accepted with reasons.
- **Spec:** 7 findings (1 wrong, 2 partial, 1 behaviour change, 3 informational /
  follow-up). Both wrong/partial findings fixed and covered by a test.
