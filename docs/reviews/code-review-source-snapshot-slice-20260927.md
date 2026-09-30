# Code review — Source snapshot slice (issue #3)

- **Date:** 2026-09-27
- **Fixed point:** `739ec6e` (branch base) — uncommitted working tree
- **Diff:** `git diff 739ec6e` (includes `etl/source_data.py`, intent-to-add)
- **Reviewed artifacts:** `etl/source_data.py` (new: GPKG inspection, canonical
  labels, stable synthetic identity), `etl/ingestion.py` (worker orchestration,
  run/stage records, staleness), `etl/extract.py`, `etl/transform.py`,
  `etl/load.py` (authoritative whole-kind load), `etl/verify.py`, `etl/marts.py`,
  `etl/db_utils.py`, `etl/utils.py`, `etl/reports.py`, `etl/__main__.py`,
  `CONTEXT.md`, `TechnicalSpecification.md`, `docs/adr/0001-core-unit-identity.md`,
  `README.md`, `tests/test_ingestion.py`, `tests/test_extract.py`,
  `tests/test_load_generators.py`, `tests/test_load_storages.py`,
  `tests/test_marts.py`, `tests/test_viz_data.py`
- **Spec:** issue #3, *Process a Source snapshot through Core and marts with
  lineage semantics* — the Source snapshot slice of parent #1, whose body is
  identical to upstream #36 ("Replace batch-only ETL with an S3/SQS event-driven
  workflow") and supplied the requirement detail behind #3's eight acceptance
  criteria. Sibling slices (#4-#11) were out of scope here: the remaining Source
  datasets and Core kinds, Boundary releases, mixed-message ordering, retry/DLQ/
  SNS/visibility, operator startup and Compose, visualization timescopes,
  Terraform, and end-to-end documentation reconciliation.
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/agents/domain.md`,
  `docs/adr/*`, `TechnicalSpecification.md`, prior reports in `docs/reviews/`
- **Verification:** `.venv/bin/python -m pytest tests/` — **361 passed** (0 failed,
  0 skipped) against the pinned `sqlalchemy==2.0.52`, plus
  `python -m compileall -q etl tests viz`.

## Standards

Three documented-standard breaches, all fixed in this slice:

1. **`TechnicalSpecification.md` §1 Extract still mandated the removed
   behaviour** — the line `- Drop duplicates` survived while the diff deleted
   `_drop_duplicate_reference_ids` and `ExtractionReport.duplicates_dropped` in
   favour of rejecting the whole snapshot. `AGENTS.md` makes the spec the single
   authoritative source, so spec and code disagreed silently. Replaced with the
   validation-before-writing rule the code actually implements.
2. **`service.source_memberships` was undocumented**, contradicting ADR 0007
   ("its purpose is recorded here, in the glossary, and in the spec's
   data-layer description"). Added the `source_memberships`, `ingestion_runs` and
   `stage_results` tables to the spec's Service section, transcribed from the
   live DDL.
3. **`README.md` still described filename discovery** (`*_V<YYYYMMDD>.gpkg`,
   "look-alikes are logged and skipped") after `FILENAME_PATTERN` /
   `_source_from_filename` were deleted in favour of content routing. Corrected.

Judgement calls, with the reasoning for what was and was not changed:

- **Duplicated Code — fixed.** Membership/staging reconciliation existed twice
  (count match, key-set match, bad-quality diff loop) in `transform.py` and
  `load.py`, and `docs/reviews/code-review-all-scope-20260915.md:24` had already
  called duplication "worst" and demanded one home. Extracted to
  `verify._verify_membership_matches_staging`, which returns the membership
  flags the load needs so it does not re-query.
- **Data Clumps — open, deliberately.** `_extract_validated_source` still takes
  `filesize`/`modified_at` and the three S3 identity values as loose optionals.
  This is the third review to raise it (`code-review-extract-v2-20260912.md:20`).
  Not taken here: it is a refactor of the local-vs-S3 seam that the next local-CLI
  slice will reshape anyway, and doing it in this slice would touch a path the
  slice does not otherwise change.
- **Speculative Generality — partly fixed.** `SourceDataset.layer_name` was read
  by nothing at all and is deleted. `SourceDataset.unit_ids` is kept: the worker
  runs `inspect_source_gpkg` on every snapshot, so the validated identity it
  returns is production output, not a test-only field — the prior "used only by
  tests" rejection does not apply to it.
- **Middle Man — open, disagreeing.** `transform_source_snapshot` forwards four
  kwargs to `_transform_source`. Kept: it is the public seam the worker calls,
  symmetric with `extract_source_snapshot` / `load_source_snapshot`, and mirrors
  the repo's existing `load_generators` / `load_source_snapshot` pattern. A thin
  public entry point over a private implementation is the convention here.
- **Primitive Obsession — open, low value.** `outcome="failed" if … else
  "succeeded"` as bare literals. `RunState` enums *run* states, not stage
  outcomes, and the literals are used consistently across `verify.py`,
  `extract.py` and `reports.py` today. An enum here would be a cross-module
  rename for no behavioural gain.
- **Mysterious Name — fixed.** `LoadReport.rows_skipped` / "Rows skipped" was
  populated with *retained historical units* while the log line, ADR 0001 and the
  spec all said "retained". Renamed to `rows_retained`.

`db_utils.py`'s call-time default-schema resolution and `marts.py`'s
`_mart_definitions(core_schema)` parameter were both confirmed correct rather
than treated as smell: tests rebind module globals via
`monkeypatch.setattr`, so a default captured in the signature would silently
defeat them. Both are now documented with that rationale inline.

One wording correction: `CONTEXT.md` claimed `input_kind` "decides which
processor handles the object". It does not — routing is by the accepted-key set
plus the injected processor. Reworded to describe it as recorded-for-reporting.

## Spec

Three implementations looked right but were wrong, all fixed:

1. **Property links were destroyed for the kind's *other* sources.** The
   whole-kind load unions every present staging source of the kind into
   `update_df`, so every unit of the kind lands in `affected`; but
   `_transfer_properties` read the decomposition from the snapshot's own staging
   table while its `DELETE` spanned **all** affected units. Loading a solar
   snapshot therefore silently stripped `location`, `technology`, `fuel_type`…
   from every wind/bio/gas/hydro unit — and because `location` is decomposed, a
   null-`reference_id` core row that lost it made the *next* load's
   `_build_core_lookup` raise, bricking the pipeline. The read now spans the
   whole kind (`present_sources`), matching the delete. Regression test
   `test_worker_keeps_other_sources_properties_when_loading_the_kind` was
   verified to fail against the old behaviour (wind lost `location` and
   `reference_source`) and pass after the fix.
2. **A failed mart refresh left earlier runs marked `succeeded`.** Only the last
   success was downgraded on `finalize()` failure, so a message with two source
   records left one claiming completion with no `marts` stage result — while
   story 10 promises dashboard data is available after ingestion. Marts are
   shared by the message, so a refresh failure now downgrades *every*
   participating run to retryable and attaches the marts failure to each. The
   single refresh is still one call; it is recorded once per run.
3. **New ack semantics dropped records silently.** Making a message
   acknowledgeable when its body parsed (to stop all-ignored messages
   redelivering forever) also deleted messages whose every record had failed to
   parse as an S3 identity. `MessageObjects.acknowledgable` is now false if any
   record was unreadable, so such a message is left for redelivery until the
   deterministic-failure path (stories 28-29) can announce it rather than being
   deleted with a record nobody claimed.

Two requirements were only partial, both now closed:

- **Story 63** — a `SourceValidationError` raised by `inspect_source_gpkg` was
  caught by the worker's generic handler and recorded `retryable` with *no*
  `stage_results` row, so a rejected snapshot had no per-table explanation.
  `_inspect` now converts it to a failed `extract` stage result.
- **Story 64** — the raw-reuse path returned before verification, so a run could
  be completed while depending on an unverified raw table. The reuse decision is
  a claim that the table is intact, so it is now verified like a freshly written
  one (with the real row count already reported).

Confirmed working, with the code path named:

- *LastModified ordering* is genuinely wired end to end. `S3ObjectId.last_modified`
  is populated only by `Boto3S3Adapter.head_current`, so the worker's
  `_resolve_current` does the single `HEAD` that both confirms the current version
  and stamps the run (`_stamp_last_modified`, before the body is read) — the SQS
  event itself carries no timestamp. The delayed-redelivery test asserts the
  recorded `terminal_error` text, so it cannot pass via the cheaper version check.
- *Idempotency ordering* — resolution happens **after** the settled-run
  short-circuit, so a redelivered already-succeeded superseded version stays
  `succeeded` rather than being flipped to `stale`.
  (`test_worker_keeps_a_redelivered_succeeded_version_succeeded`.)
- *Reference date* is descriptive only; the freshness gate is gone from the code
  and from ADR 0001 and the spec.
- *Bad quality* — a present bad-quality row creates no new Core row, an existing
  row is retained when a later snapshot degrades it, and membership keeps the
  lineage (`test_worker_keeps_core_when_a_unit_turns_bad_quality`).
- *Validation precedes mutation* — an invalid snapshot leaves the previous Core
  rows, the raw tables and the run ledger untouched
  (`test_worker_rejects_an_invalid_snapshot_without_touching_the_database`).

### Out-of-scope defect found while reviewing (resolved by a pin)

`scripts/seed_data_volume.sh:60` writes `VIZ_DATABASE_URL=postgresql://…` with
no explicit driver, and `scripts/smoke_etl_container.sh:185` asserts that bare
prefix. `requirements-viz.txt` installs `psycopg2-binary`, so this only works
while SQLAlchemy's bare `postgresql://` defaults to **psycopg2** — which is true
on 2.0.x but not on ≥2.1, where it resolves to **psycopg3**, a driver the viz
image does not install. A freshly built viz container on 2.1 would fail to
connect, and the smoke test would not catch it.

Resolved outside this slice by `e219381`, which pins `sqlalchemy==2.0.52` in both
`requirements.txt` and `requirements-viz.txt`; the full suite was re-run against
that pin (361 passed). `tests/test_viz_data.py` was still made explicit
(`postgresql+psycopg2://`) so the test no longer depends on which driver the
library happens to default to. Worth doing in the operator startup and Compose
slice (#8): make the driver explicit in the seed script and the smoke assertion
too, so a future SQLAlchemy bump cannot silently reintroduce the failure.

## Summary

- **Standards:** 9 findings (3 hard breaches, 6 judgement calls) — worst was the
  spec's stale `- Drop duplicates` line, which contradicted the code the diff
  deleted. All 3 hard breaches fixed; 3 of 6 judgement calls fixed, 3 accepted
  with a stated reason.
- **Spec:** 8 findings (3 wrong, 2 partial, 3 out-of-scope/informational) — worst
  was the whole-kind property transfer stripping decomposed properties from every
  other source of the kind, a data-corruption bug with a self-inflicted deadlock.
  All 5 in-scope findings fixed.
