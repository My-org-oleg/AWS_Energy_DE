# Code review — Blocked-startup alerts and mart definition drift (issue #1)

- **Date:** 2026-10-01
- **Fixed point:** `2a5f2fa` (branch `main` HEAD) — uncommitted working tree
- **Diff:** `git diff 2a5f2fa`
- **Reviewed artifacts:** `etl/ingestion.py` (`REFUSAL_*`, `_REFUSAL_SUBJECTS`,
  `_REFUSAL_REMEDIES`, `run_startup`, `_refused`, `_refusal_fingerprint`,
  `_alert_bootstrap`, `_release_bootstrap_claim`, `_clear_bootstrap_claims`,
  `_bootstrap_alert`, `_ensure_service_metadata`, `Boto3SNSAdapter`),
  `etl/marts.py` (`_ACTIVE`, `_fingerprint`, `_ensure_fingerprints`,
  `_present_matviews`, `_create_mart`, `_create_marts`, `_replace_mart`,
  `_record_fingerprints`, `build_marts`), `etl/reports.py` (`MartsReport.recreated`),
  `etl/__main__.py` (the `startup` command), `tests/test_ingestion.py`,
  `tests/test_marts.py`, `tests/test_acceptance_workflow.py`, `CONTEXT.md`
  (**Service**), `TechnicalSpecification.md` (`bootstrap_alerts`,
  `definition_fingerprints`), `AWS.md`, `docs/containerization.md`,
  `compose.yaml`, `build_compose.yaml`, `.env.example`,
  `docs/adr/0003-marts-as-stored-wide-pivots.md`,
  `docs/adr/0007-service-schema-operational-metadata.md`,
  `docs/adr/0009-server-startup-bootstrap-and-redrive.md`
- **Spec:** issue #1, *ETL platform for renewable energy units* — user stories 21
  and 22 and the `marts` bullet of the schema layout. The blocked-startup alert
  is an audit finding against the deployment rather than issue #1 text; its
  spec-of-record is the decision recorded in ADR 0009 and `AWS.md`.
- **Standards sources:** `AGENTS.md`, `CONTEXT.md`, `docs/agents/domain.md`,
  `docs/adr/*`, `TechnicalSpecification.md`, prior reports in `docs/reviews/`
- **Verification:** `.venv/bin/python -m pytest` — **550 passed**;
  `python -m compileall -q etl viz tests` clean; `terraform fmt -check` clean;
  `tests/test_terraform_config.py tests/test_compose_config.py` — 50 passed

## Standards

Findings as returned by the Standards sub-agent, and what was done about each.

1. **Hard — two new tables, neither in the spec.** `service.bootstrap_alerts` and
   `marts.definition_fingerprints` are table-schema changes, and `AGENTS.md` makes
   `TechnicalSpecification.md` the single authoritative source for table schemas.
   **Fixed:** both added with full column tables, under §2 Service and §5 Marts.
2. **Hard-ish — glossary drift.** `CONTEXT.md`'s **Service** entry is a closed
   enumeration and omitted `bootstrap_alerts`; ADR 0007 requires the schema's
    contents to be recorded in the glossary. **Fixed:** added, alongside a new
    **Startup alert claim** glossary term.
3. **Hard-ish — unflagged ADR conflict.** ADR 0007 draws the line "`service` is
   operational metadata, not a pipeline data layer", and
   `etl/marts.py::_ensure_fingerprints` argued the opposite without ADR 0007
   acknowledging the reversal; `docs/agents/domain.md` requires such a conflict to
   be surfaced explicitly. **Fixed:** ADR 0007 now records the three extensions to
   `service` and names `marts.definition_fingerprints` as the one deliberate
   exception, with the reason; ADR 0003 cross-references it.
4. **House style — report column.** `etl/__main__.py` put the `Alert published`
   colon at column 20 where every sibling line uses 19, and a test pinned the
   misaligned string. **Fixed:** realigned, the two tests updated, and the two
   consecutive `if not result.worker_start_allowed:` blocks folded into one.
5. **Mislabelled alert (the substantive one).** `REFUSAL_BOUNDARY_RELEASE` guarded
   a `try` wrapping *both* `_apply_boundary_releases` **and**
   `_enqueue_current_sources`, so an unreachable queue announced "Boundary release
   rejected", and the body appended an unconditional "this is about the Boundary
   reference layer" — wrong for a database-down precondition too. **Fixed:** the two
   steps are now refused separately under a fourth reason, `REFUSAL_ENQUEUE`, and
   each reason carries its own remedy from `_REFUSAL_REMEDIES`; a new test
   (`test_a_queue_failure_does_not_blame_the_boundary_release`) fails on the old
   shared `except`.
6. **Duplicated Code.** `_missing_required_keys(checks)` was computed in
   `_alert_bootstrap` and recomputed inside `_refusal_fingerprint`, which also
   received `checks`. **Fixed:** `_refusal_fingerprint` now takes the already
   computed `missing` tuple.
7. **Data Clump.** `bucket, reason, checks, error[, missing]` travels
   `_refused` → `_alert_bootstrap` → `_bootstrap_alert`/`_refusal_fingerprint`;
   one `_Refusal` value would carry it. **Not fixed** — accepted, matching the
   recorded precedent in `code-review-mixed-message-ordering-20260928.md` S7. The
   clump is four values across three private helpers on one call path, and the
   bundle would add a type without removing a branch.
8. **Transaction regression.** `_create_marts` opened four transactions
   (`_present_matviews`, DROP, CREATE, fingerprint upsert) where the old loop had
   one; a failure between DROP and CREATE left a pivot missing. **Fixed:** extracted
   `_replace_mart`, which drops and creates in a single transaction, so a failed
   replacement leaves the old view in place. The remaining split is read-only
   plus one idempotent upsert.
9. **Vacuous assertion.** `assert "level" in " ".join(sns.bodies.split())` passed
   regardless of the reason, since every boundary key contains "level".
   **Fixed:** the test now asserts the specific failing key, the *absence* of a
   `Missing:` line on the all-keys-present path, and that the missing-boundary
   remedy is not in the body.
10. **Nits.** Stray double blank lines in `tests/test_marts.py`. **Fixed.**

### Verified correct (Standards)

The README's test count (550 = 535 + 15). `_ACTIVE` agreeing with
`viz/data.py` on the decommissioning bound. The `drop_all_pipeline_data.sql`
claim. "No fingerprint = stale" as the right default. `CONTEXT.md` already
carried both Active bounds, so the marts fix aligns code to the glossary rather
than needing a glossary edit. The acceptance walkthrough threading `sns`.

## Spec

Findings as returned by the Spec sub-agent, and what was done about each.

1. **Implemented but wrong — `REFUSAL_BOUNDARY_RELEASE` mislabelled a third of its
   own causes.** Same defect as Standards 5, found independently. **Fixed** as
   above; the new test is the regression guard.
2. **Implemented but wrong — the dedup claim was permanent with no remedy.** The
   agreed design was implemented faithfully, but the only `DELETE` was the
   failed-publish release, so a *recurrence months later* of an identical condition
   was silent forever — exactly the crash loop the alert exists to report, reported
   to nobody. **Fixed:** `_clear_bootstrap_claims` clears a bucket's claims on a
   successful startup, so the boundary between one incident and the next is the
   fix. Best effort, never fatal. Covered by
   `test_a_refusal_alerts_again_after_the_condition_is_fixed`; ADR 0009 and
   `AWS.md` record that "once per distinct condition" means once per incident.
3. **Implemented but wrong — the "marts and map agree" claim was false.** The
   `etl/marts.py` comment and ADR 0003 claimed the mart predicate is
   `viz.data.ACTIVE_UNIT_PREDICATE` evaluated at today, but that predicate spans
   `commissioning_date BETWEEN :from AND :to`, which at a single day means
   commissioned *exactly* today. **Fixed:** both now state it is the timescope
   rule with its interval collapsed to today and explicitly *not* a verbatim copy,
   and say why the difference does not matter in practice.
4. **Unrecorded NULL consequence.** `commissioning_date` is nullable and only the
   *pair* is quality-checked, so `NULL <= CURRENT_DATE` drops such units from all
   three pivots — a real narrowing from the old one-sided rule, unrecorded.
   **Fixed:** documented in the `_ACTIVE` comment and ADR 0003, noting it is
   deliberate and agrees with the visualization.
5. **Missing — topic-description prose half-updated.** Three places still said
   "ingestion and DLQ alerts": `etl/__main__.py` (worker command),
   `compose.yaml:111`, `build_compose.yaml:68`, `docs/containerization.md:352`.
   **Fixed:** all four now name the blocked-startup path.
6. **Scope creep.** None found. `report.recreated`, the `Alert published` line and
   the reason-map guard test are all in service of the two agreed designs.

### Answers to the specific questions put to the sub-agent

- **Independent predicate copies in the tests:** defensible, and explicitly
  justified in `tests/test_marts.py`. They pin expected values against a rule that
  must not move with the code, and the one-directional drift is caught by
  `test_every_pivot_filters_on_the_same_active_rule` plus the two new bound
  assertions. The remaining old-predicate use is the deliberate stale definition.
- **Pre-fingerprint deployments:** covered. `recorded.get(name)` returning `None`
  routes "no row" into the recreate branch, and
  `test_a_view_with_no_recorded_fingerprint_counts_as_stale` asserts all three
  recreate and that the *next* build is quiet. The absent-*table* half is covered
  by `CREATE TABLE IF NOT EXISTS` on first build.
- **SNS reachability and failure:** holds. `terraform/iam.tf` already grants
  `sns:Publish` and `startup` already required `SNS_TOPIC_ARN`, so "no
  Terraform/IAM changes" is accurate. Failure is silent and does not change the
  refusal, per `test_a_failed_bootstrap_alert_does_not_change_the_refusal`.

## Summary

Standards: 10 findings, worst being the shared `except` that mislabelled an
enqueue failure as a Boundary rejection (fixed, with a regression test).
Spec: 6 findings, worst being the permanent alert claim that would silence a
recurrence of the same outage forever (fixed by clearing claims on success).
Both axes independently found the mislabelled alert, which is the strongest
signal in the review; both also found documentation lagging the code, which is
consistent with this repo's recorded history. No finding was left unaddressed
except the `Data Clump` bundle (S7), accepted on a precedent already recorded in
`docs/reviews/`.
